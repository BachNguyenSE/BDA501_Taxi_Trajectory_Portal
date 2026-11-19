"""Đo độ trễ REST POST /estimate và chạy F3 qua API (20 chuyến mẫu tập test, so dự đoán với thực tế).

Chạy (API phải đang chạy): docker compose run --rm --no-deps spark python src/checks/api_latency.py
Gọi http://api:8000 trong mạng Docker. Độ trễ đo phía client: gồm HTTP, kiểm tra input, feature, model GBT,
tìm tuyến Dijkstra, tra mức tắc và lớp live trong MongoDB.
Ghi: evidence/serving/api_latency.json
"""
import itertools
import json
import random
import statistics
import time

import httpx

from core.common import ROOT, load_config

URL = "http://api:8000"


def main():
    cfg = load_config()
    lm = [x for x in json.loads((ROOT / cfg["ui"]["landmarks"]).read_text(encoding="utf-8")) if x["ok"]]
    samples = json.loads((ROOT / cfg["ui"]["sample_trips"]).read_text(encoding="utf-8"))
    rnd = random.Random(cfg["project"]["seed"])
    pairs = rnd.sample(list(itertools.permutations(lm, 2)), 50)
    hours = [2, 8, 12, 17, 18, 22]
    with httpx.Client(base_url=URL, timeout=30) as c:
        health = c.get("/health").json()
        for _ in range(5):   # khởi động (đồ thị theo giờ được dựng và cache ở lần đầu), không tính
            c.post("/estimate", json={"origin": {"lat": lm[0]["lat"], "lon": lm[0]["lon"]},
                                      "destination": {"lat": lm[1]["lat"], "lon": lm[1]["lon"]},
                                      "departure_time": "2014-05-14T17:30:00"})
        lat_ms = []
        for i, (a, b) in enumerate(pairs):
            body = {"origin": {"lat": a["lat"], "lon": a["lon"]}, "destination": {"lat": b["lat"], "lon": b["lon"]},
                    "departure_time": f"2014-05-14T{hours[i % len(hours)]:02d}:15:00"}
            t = time.perf_counter()
            r = c.post("/estimate", json=body)
            lat_ms.append((time.perf_counter() - t) * 1000)
            r.raise_for_status()
        f3 = []
        for s in samples:
            body = {"origin": {"lat": s["origin"][0], "lon": s["origin"][1]},
                    "destination": {"lat": s["destination"][0], "lon": s["destination"][1]},
                    "departure_time": s["start_local"].replace(" ", "T")}
            r = c.post("/estimate", json=body).json()
            lo, hi = r["predicted_interval_min"]
            f3.append({"trip_uid": s["trip_uid"], "departure_local": s["start_local"], "actual_min": s["actual_duration_min"],
                       "predicted_min": r["predicted_duration_min"], "interval_min": [lo, hi],
                       "actual_in_interval": lo <= s["actual_duration_min"] <= hi, "route_zones": len(r["route"])})
        bad = c.post("/estimate", json={"origin": {"lat": 41.2, "lon": -8.6}}).status_code
    lat_ms.sort()
    out = {
        "health": health,
        "latency_ms": {"n": len(lat_ms), "p50": round(statistics.median(lat_ms), 2),
                       "p95": round(lat_ms[int(0.95 * len(lat_ms)) - 1], 2), "max": round(lat_ms[-1], 2),
                       "note": "50 cặp địa điểm có sẵn chọn ngẫu nhiên (seed 42), 6 khung giờ khác nhau; 5 lần khởi động không tính"},
        "f3_via_api": {"trips": f3,
                       "mae_min": round(sum(abs(x["predicted_min"] - x["actual_min"]) for x in f3) / len(f3), 2),
                       "actual_in_interval": sum(x["actual_in_interval"] for x in f3),
                       "note": "20 chuyến mẫu đã bỏ chuyến nhiễu GPS và chuyến dưới 1 km: chỉ minh họa, số đánh giá chính thức ở E_model_validation.md"},
        "invalid_request_status": bad,
    }
    ev = ROOT / "evidence" / "serving"
    ev.mkdir(parents=True, exist_ok=True)
    (ev / "api_latency.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in out.items() if k != "f3_via_api"}, ensure_ascii=False, indent=1))
    print("F3 qua API: MAE", out["f3_via_api"]["mae_min"], "· thực tế nằm trong khoảng:", out["f3_via_api"]["actual_in_interval"], "/", len(f3))


if __name__ == "__main__":
    main()
