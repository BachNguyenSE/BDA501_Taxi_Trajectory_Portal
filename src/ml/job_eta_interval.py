"""Job: tính KHOẢNG ƯỚC LƯỢNG 80% cho ETA (mở rộng ngoài đề, đã duyệt).

Mục trong sơ đồ : 7, 8 (bổ sung cho model ETA).
Làm gì          : 1. GBT cùng tham số với model phục vụ, chỉ học chuyến trước 01/04/2014
                  2. sai số (thực tế − dự đoán) trên tháng hiệu chỉnh 04/2014 (vẫn thuộc giai đoạn train)
                  3. chia 10 nhóm theo độ dài dự đoán; mỗi nhóm lấy phân vị 10% và 90% của sai số
                  4. áp bảng lên model PHỤC VỤ, đo độ bao phủ trên tập test 05-06/2014 MỘT lần
                  Quy tắc (80%, 10 nhóm) chốt trước khi chạy, không chỉnh theo kết quả test.
Đọc             : data/gold/trip_features, models/features.json, model phục vụ
Ghi             : models/interval.json, evidence/ml/interval_validation.json
Chạy            : tự gọi trong scripts/run_ml.sh --full  (bên trong: python src/ml/job_eta_interval.py)
Lưu ý           : tra khoảng lúc phục vụ bằng src/ml/interval.py (hàm lookup).
"""
import json

import numpy as np
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressionModel, GBTRegressor
from pyspark.sql import functions as F

from core.common import ROOT, get_spark, load_config
from ml.interval import lookup
from ml.job_train_eta import GBT_PARAMS

COVERAGE, N_BINS = 0.80, 10


def main():
    cfg = load_config()
    spark = get_spark(cfg, "job-eta-interval")
    spark.sparkContext.setLogLevel("WARN")
    feats, label = cfg["ml"]["features"], cfg["ml"]["label"]
    cut, vfrom = cfg["split"]["train_before"], cfg["split"]["validation_from"]
    meta = json.loads((ROOT / cfg["paths"]["models"] / "features.json").read_text(encoding="utf-8"))

    tf = spark.read.parquet(str(ROOT / "data" / "gold" / "trip_features"))
    for c in ["is_weekend", "is_holiday"]:
        tf = tf.withColumn(c + "_f", F.col(c).cast("double"))
    data = VectorAssembler(inputCols=meta["feature_vector_columns"], outputCol="features").transform(tf)
    ts = F.col("start_ts_local")
    fit_set = data.filter(ts < F.lit(vfrom).cast("timestamp"))
    calib = data.filter((ts >= F.lit(vfrom).cast("timestamp")) & (ts < F.lit(cut).cast("timestamp")))
    test = data.filter(F.col("split") == "test")

    cal_model = GBTRegressor(featuresCol="features", labelCol=label, predictionCol="pred", **GBT_PARAMS).fit(fit_set)
    c = cal_model.transform(calib).select(label, "pred").toPandas()
    pred, resid = c["pred"].to_numpy(), (c[label] - c["pred"]).to_numpy()
    edges = np.quantile(pred, np.linspace(0, 1, N_BINS + 1))
    edges[0], edges[-1] = -np.inf, np.inf
    lo_q, hi_q = (1 - COVERAGE) / 2, 1 - (1 - COVERAGE) / 2
    q_low, q_high, cal_n = [], [], []
    for i in range(N_BINS):
        m = (pred >= edges[i]) & (pred < edges[i + 1])
        q_low.append(float(np.quantile(resid[m], lo_q)))
        q_high.append(float(np.quantile(resid[m], hi_q)))
        cal_n.append(int(m.sum()))
    table = {"coverage_target": COVERAGE, "bin_edges": [float(x) if np.isfinite(x) else (-1e9 if x < 0 else 1e9) for x in edges[:-1]],
             "q_low": q_low, "q_high": q_high, "min_duration_min": 0.25,
             "calibration": {"model_trained_before": vfrom, "calibration_month": f"{vfrom} .. {cut}",
                             "calibration_trips": int(len(c)), "trips_per_bin": cal_n, "gbt_params": GBT_PARAMS},
             "applies_to_model": meta["serving_model"]}
    (ROOT / cfg["paths"]["models"] / "interval.json").write_text(json.dumps(table, indent=2), encoding="utf-8")

    # ---- đo một lần trên test, đúng model phục vụ
    serving = GBTRegressionModel.load(str(ROOT / cfg["paths"]["models"] / meta["serving_model"]))
    t = serving.transform(test).select(label, "pred", "has_gps_jump").toPandas()
    iv = np.array([lookup(table, p) for p in t["pred"]])
    inside = (t[label].to_numpy() >= iv[:, 0]) & (t[label].to_numpy() <= iv[:, 1])
    width = iv[:, 1] - iv[:, 0]
    bins = np.clip(np.searchsorted(np.array(table["bin_edges"]), t["pred"].to_numpy(), side="right") - 1, 0, N_BINS - 1)
    per_bin = []
    for i in range(N_BINS):
        m = bins == i
        per_bin.append({"bin": i, "pred_from_min": None if i == 0 else round(table["bin_edges"][i], 2),
                        "pred_to_min": None if i == N_BINS - 1 else round(table["bin_edges"][i + 1], 2),
                        "q_low": round(q_low[i], 2), "q_high": round(q_high[i], 2), "calibration_trips": cal_n[i],
                        "test_trips": int(m.sum()), "test_coverage": round(float(inside[m].mean()), 4),
                        "mean_width_min": round(float(width[m].mean()), 2)})
    j = t["has_gps_jump"].to_numpy()
    out = {"target": COVERAGE, "test_trips": int(len(t)), "test_coverage": round(float(inside.mean()), 4),
           "test_coverage_no_gps_jump": round(float(inside[~j].mean()), 4),
           "below_interval": round(float((t[label].to_numpy() < iv[:, 0]).mean()), 4),
           "above_interval": round(float((t[label].to_numpy() > iv[:, 1]).mean()), 4),
           "mean_width_min": round(float(width.mean()), 2), "median_width_min": round(float(np.median(width)), 2),
           "per_bin": per_bin, "calibration": table["calibration"]}
    ev = ROOT / "evidence" / "ml"
    (ev / "interval_validation.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in out.items() if k != "per_bin"}, indent=2))
    for b in per_bin:
        print(b)
    spark.stop()


if __name__ == "__main__":
    main()
