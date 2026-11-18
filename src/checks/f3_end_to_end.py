"""Card F3: chạy thật chuyến từ CSV gốc đến estimate(), so dự đoán với thực tế.

Với mỗi chuyến mẫu (data/ui/sample_trips.json, tập test), đọc LẠI dòng gốc trong data/raw/train.csv
theo TRIP_ID, tự tính A, B, giờ xuất phát (Lisbon) và thời lượng thật từ POLYLINE, rồi gọi estimate().
Model phục vụ chỉ huấn luyện trên dữ liệu trước 05/2014, nên đây là dự đoán ngoài mẫu.
"""
import csv
import json
from datetime import datetime
from zoneinfo import ZoneInfo

from core.common import ROOT, load_config
from serving.estimate import Estimator


def main():
    cfg = load_config()
    tz = ZoneInfo(cfg["project"]["timezone_local"])
    samples = json.loads((ROOT / cfg["ui"]["sample_trips"]).read_text(encoding="utf-8"))
    want = {s["trip_uid"].split("-")[0]: s for s in samples}
    csv.field_size_limit(10 ** 9)
    raw = {}
    with open(ROOT / cfg["paths"]["raw_csv"], newline="") as f:
        for row in csv.reader(f):
            if row[0] in want:
                raw.setdefault(row[0], []).append(row)
    est = Estimator(cfg)
    out = []
    for tid, s in want.items():
        rows = raw.get(tid, [])
        pts = json.loads(rows[0][8])
        dep = datetime.fromtimestamp(int(rows[0][5]), tz).replace(tzinfo=None)
        actual = (len(pts) - 1) * cfg["cleaning"]["gps_interval_s"] / 60
        res = est.estimate((pts[0][1], pts[0][0]), (pts[-1][1], pts[-1][0]), dep)
        out.append({
            "trip_id": tid, "raw_rows_with_this_id": len(rows),
            "departure_local": dep.strftime("%Y-%m-%d %H:%M:%S"),
            "origin_lat_lon": [pts[0][1], pts[0][0]], "destination_lat_lon": [pts[-1][1], pts[-1][0]],
            "actual_duration_min": round(actual, 2), "predicted_duration_min": res["predicted_duration_min"],
            "error_min": round(res["predicted_duration_min"] - actual, 2),
            "route_zones": len(res["route"]), "congested_zones": res["congested_zones"],
            "live_available": res["live"]["available"],
            "matches_sample_file": abs(actual - s["actual_duration_min"]) < 1e-6 and dep.strftime("%Y-%m-%d %H:%M:%S") == s["start_local"],
        })
    errs = [abs(x["error_min"]) for x in out]
    summary = {"n_trips": len(out), "mae_min_on_these_trips": round(sum(errs) / len(errs), 2),
               "note": "MAE trên 20 chuyến mẫu chỉ minh họa; số đánh giá chính thức là MAE trên toàn tập test (E_model_validation.md)."}
    ev = ROOT / "evidence" / "serving"
    ev.mkdir(parents=True, exist_ok=True)
    first = est.estimate(tuple(out[0]["origin_lat_lon"]), tuple(out[0]["destination_lat_lon"]),
                         datetime.strptime(out[0]["departure_local"], "%Y-%m-%d %H:%M:%S"))
    (ev / "F3_end_to_end.json").write_text(json.dumps({"summary": summary, "trips": out, "full_response_first_trip": first},
                                                      ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False))
    for x in out:
        print(x["trip_id"], x["departure_local"], "thực tế", x["actual_duration_min"], "dự đoán", x["predicted_duration_min"],
              "zone", x["route_zones"], "khớp file mẫu", x["matches_sample_file"])


if __name__ == "__main__":
    main()
