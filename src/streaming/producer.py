"""Replay producer (mục 12): phát lại GPS từ raw CSV vào Kafka theo thứ tự thời gian sự kiện.

- Khung: streaming.replay.window_local (giờ Lisbon), tốc độ ×speedup (1 giây thật = 1 phút dữ liệu).
- Mỗi message là một điểm GPS kèm điểm trước đó (prev_lon, prev_lat), tức một đoạn 15 giây.
  event_time = thời điểm của điểm hiện tại (epoch giây, UTC).
- acks=all, key = TAXI_ID (giữ thứ tự theo từng xe trong một partition).
- CỐ Ý chèn dữ liệu xấu, mỗi message gốc chỉ có MỘT số phận (chọn bằng random seed 42):
    corrupt  : gửi JSON hỏng thay cho message gốc
    late     : gửi trễ late_delay_min phút thời gian sự kiện
    duplicate: gửi message gốc và một bản sao giống hệt ngay sau đó
    normal   : gửi một lần, đúng giờ
- Cuối cùng gửi một message kết thúc (end-of-replay) có event_time sau khung 1 giờ, tọa độ ngoài bbox:
  nó đẩy watermark qua mọi cửa sổ để Spark xuất các cửa sổ cuối (chế độ append), và bị lọc khỏi phép tổng hợp.
- Ghi manifest với SỐ LƯỢNG THẬT đã gửi, kèm số kỳ vọng sau lọc để đối soát với Spark.
"""
import argparse
import csv
import json
import math
import random
import time
from collections import Counter
from datetime import datetime
from zoneinfo import ZoneInfo

from core.common import ROOT, load_config

EARTH_R_KM = 6371.0


def haversine_km(lon1, lat1, lon2, lat2):
    a = (math.sin((math.radians(lat2) - math.radians(lat1)) / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin((math.radians(lon2) - math.radians(lon1)) / 2) ** 2)
    return 2 * EARTH_R_KM * math.asin(math.sqrt(a))


def in_bbox(lon, lat, b):
    return b["lon_min"] <= lon <= b["lon_max"] and b["lat_min"] <= lat <= b["lat_max"]


def passes_filter(m, cfg):
    """Cùng điều kiện với batch và với stream_job: hai đầu trong bbox, tốc độ <= max_speed_kmh."""
    b, c = cfg["grid"]["bbox"], cfg["cleaning"]
    if not (in_bbox(m["prev_lon"], m["prev_lat"], b) and in_bbox(m["lon"], m["lat"], b)):
        return False
    return haversine_km(m["prev_lon"], m["prev_lat"], m["lon"], m["lat"]) * 3600.0 / c["gps_interval_s"] <= c["max_speed_kmh"]


def window_bounds(cfg):
    tz = ZoneInfo(cfg["project"]["timezone_local"])
    w = cfg["streaming"]["replay"]["window_local"]
    f = "%Y-%m-%d %H:%M"
    return (int(datetime.strptime(w["start"], f).replace(tzinfo=tz).timestamp()),
            int(datetime.strptime(w["end"], f).replace(tzinfo=tz).timestamp()))


def load_segments(cfg, csv_path):
    """Đoạn (điểm + điểm trước) có event_time trong khung replay, đọc từ raw CSV."""
    w0, w1 = window_bounds(cfg)
    dt = cfg["cleaning"]["gps_interval_s"]
    segs, trips = [], Counter()
    csv.field_size_limit(10 ** 9)
    with open(csv_path, newline="") as f:
        for row in csv.reader(f):
            if row[0] == "TRIP_ID" or row[7] == "True":
                continue
            start = int(row[5])
            if start >= w1 or start < w0 - 86400:   # một chuyến không kéo dài quá 1 ngày
                continue
            try:
                pts = json.loads(row[8])
            except ValueError:
                continue
            hit = False
            for i in range(1, len(pts)):
                t = start + i * dt
                if w0 <= t < w1:
                    segs.append({"trip_id": row[0], "taxi_id": int(row[4]), "seq": i, "event_time": t,
                                 "lon": pts[i][0], "lat": pts[i][1], "prev_lon": pts[i - 1][0], "prev_lat": pts[i - 1][1]})
                    hit = True
            if hit:
                trips[row[0]] += 1
    shared = sorted(k for k, v in trips.items() if v > 1)
    return segs, {"rows_with_points_in_window": sum(trips.values()), "trip_ids": len(trips),
                  "trip_ids_shared_by_several_rows": shared}


def build_plan(segs, cfg, seed):
    """Danh sách (send_clock, key, value_bytes, kind) theo thứ tự gửi, và manifest."""
    r = cfg["streaming"]["replay"]["bad_data"]
    rng = random.Random(seed)
    late_s = r["late_delay_min"] * 60
    p_c, p_l, p_d = r["corrupt_json_rate"], r["late_rate"], r["duplicate_rate"]
    plan, n = [], Counter()
    exp = Counter()
    for m in sorted(segs, key=lambda x: (x["event_time"], x["trip_id"], x["seq"])):
        key = str(m["taxi_id"]).encode()
        val = json.dumps(m, separators=(",", ":")).encode()
        u = rng.random()
        ok = passes_filter(m, cfg)
        if u < p_c:
            kind = "corrupt"
            plan.append((m["event_time"], key, val[: len(val) // 2], "corrupt"))
        elif u < p_c + p_l:
            kind = "late"
            plan.append((m["event_time"] + late_s, key, val, "late"))
        elif u < p_c + p_l + p_d:
            kind = "duplicate"
            plan.append((m["event_time"], key, val, "duplicate_original"))
            plan.append((m["event_time"], key, val, "duplicate_copy"))
        else:
            kind = "normal"
            plan.append((m["event_time"], key, val, "normal"))
        n[kind] += 1
        exp[f"{kind}_pass_filter"] += int(ok)
    last = max(p[0] for p in plan)
    w0, w1 = window_bounds(cfg)
    sentinel = {"trip_id": "END_OF_REPLAY", "taxi_id": 0, "seq": 0, "event_time": w1 + 3600,
                "lon": 0.0, "lat": 0.0, "prev_lon": 0.0, "prev_lat": 0.0}
    plan.append((last + 1, b"0", json.dumps(sentinel).encode(), "sentinel"))
    plan.sort(key=lambda p: p[0])   # sort ổn định: giữ thứ tự gốc khi cùng send_clock (bản sao ngay sau bản gốc)
    manifest = {
        "original_messages": len(segs),
        "fate_counts": dict(n),
        "messages_sent_total": len(plan),
        "corrupt_json_sent": n["corrupt"],
        "late_sent": n["late"],
        "duplicate_copies_sent": n["duplicate"],
        "sentinel_sent": 1,
        "expected": {
            # message hợp lệ, đúng giờ, qua bộ lọc (bản sao chỉ tính một lần)
            "segments_aggregated_if_all_late_dropped": exp["normal_pass_filter"] + exp["duplicate_pass_filter"],
            "late_messages_that_would_pass_filter": exp["late_pass_filter"],
            "valid_json_messages": len(plan) - n["corrupt"],
        },
        "rates_config": r, "seed": seed, "late_delay_s": late_s,
    }
    return plan, manifest


def replay(plan, send, speedup, sleep=time.sleep, now=time.monotonic):
    """Gửi theo đồng hồ thời gian sự kiện, nén ×speedup. Trả về số đã gửi."""
    clock0, real0 = plan[0][0], now()
    for clock, key, val, _ in plan:
        wait = (clock - clock0) / speedup - (now() - real0)
        if wait > 0:
            sleep(wait)
        send(key, val)
    return len(plan)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--speedup", type=float, default=None)
    args = ap.parse_args()
    cfg = load_config()
    st = cfg["streaming"]
    from kafka import KafkaProducer

    t0 = time.time()
    segs, info = load_segments(cfg, ROOT / cfg["paths"]["raw_csv"])
    plan, manifest = build_plan(segs, cfg, cfg["project"]["seed"])
    manifest.update({"window_local": st["replay"]["window_local"], "input": info,
                     "speedup": args.speedup or st["replay"]["speedup"], "topic": st["kafka"]["topic"],
                     "key": "TAXI_ID", "acks": st["kafka"]["producer_acks"],
                     "load_seconds": round(time.time() - t0, 1)})
    prod = KafkaProducer(bootstrap_servers=st["kafka"]["bootstrap"], acks=st["kafka"]["producer_acks"],
                         linger_ms=5, retries=5)
    errors = []

    def send(key, val):
        prod.send(st["kafka"]["topic"], key=key, value=val).add_errback(lambda e: errors.append(str(e)))

    t1 = time.time()
    sent = replay(plan, send, manifest["speedup"])
    prod.flush()
    prod.close()
    manifest.update({"messages_handed_to_kafka": sent, "send_errors": len(errors),
                     "replay_wall_seconds": round(time.time() - t1, 1)})
    out = ROOT / st["replay"]["manifest"]
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    (ROOT / "data" / "stream").mkdir(parents=True, exist_ok=True)
    (ROOT / "data" / "stream" / "producer_done.json").write_text(json.dumps({"sent": sent}), encoding="utf-8")
    print(json.dumps({k: manifest[k] for k in ["original_messages", "fate_counts", "messages_sent_total",
                                               "send_errors", "replay_wall_seconds"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
