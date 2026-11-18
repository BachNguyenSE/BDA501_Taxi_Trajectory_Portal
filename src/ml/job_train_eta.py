"""Job: HUẤN LUYỆN và ĐÁNH GIÁ model dự đoán thời gian chuyến (known-destination ETA).

Mục trong sơ đồ : 7 (Spark ML), 8 (models/).
Làm gì          : 2 baseline (naive, tốc độ lịch sử) + Linear Regression + GBT trên đúng 6 feature của đề;
                  chia theo thời gian (train trước 01/05/2014 giờ Lisbon, test 05-06/2014);
                  báo cáo cả có lọc và không lọc chuyến có bước nhảy GPS; kiểm tra leakage tự động
                  (script DỪNG nếu vi phạm); lưu model và features.json.
Đọc             : data/gold/trip_features, data/silver/segments (chỉ phần trước mốc chia, cho baseline)
Ghi             : evidence/ml/model_results_<mode>.json, evidence/ml/leakage_checks_<mode>.json,
                  models/spark_*/ và models/features.json (chỉ khi --mode full)
Chạy            : scripts/run_ml.sh [--sample|--full]  (bên trong: python src/ml/job_train_eta.py --mode ...)
Lưu ý           : tham số model cố định trước khi chạy, không tinh chỉnh; tập test chỉ dùng để báo cáo.
                  Nguyên tắc chống leakage: docs/DATA_LEAKAGE.md. Hàm dùng chung: src/ml/evaluation.py.
"""
import argparse
import json
import shutil
import time

from pyspark import StorageLevel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.sql import functions as F

from core.common import ROOT, first_row, get_spark, load_config
from ml.evaluation import FORBIDDEN_FEATURES, check_features, evaluate, historical_speed_baseline, historical_speed_tables

# Tham số cố định, chọn trước khi chạy, không tinh chỉnh theo kết quả
GBT_PARAMS = {"maxIter": 50, "maxDepth": 6, "stepSize": 0.1, "maxBins": 32, "seed": 42}
LR_PARAMS = {"regParam": 0.0, "elasticNetParam": 0.0}
# Model phục vụ chọn trước (đúng đề, không dựa trên kết quả test): GBT huấn luyện trên toàn bộ tập train
SERVING = ("gbt", "train_all")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["sample", "full"], default="sample")
    m = ap.parse_args().mode
    cfg = load_config()
    spark = get_spark(cfg, f"job-train-eta-{m}")
    spark.sparkContext.setLogLevel("WARN")
    ev = ROOT / "evidence" / "ml"
    ev.mkdir(parents=True, exist_ok=True)
    base = ROOT / ("data/sample" if m == "sample" else "data")
    feats, label = cfg["ml"]["features"], cfg["ml"]["label"]
    cut, dt = cfg["split"]["train_before"], cfg["cleaning"]["gps_interval_s"]
    t_all = time.time()

    check_features(feats)
    tf = spark.read.parquet(str(base / "gold" / "trip_features"))
    data = (tf.filter(F.col("split").isin("train", "test"))
            .select("trip_uid", "split", "start_ts_local", "origin_zone", "has_gps_jump", label, *feats))
    for c in ["is_weekend", "is_holiday"]:
        data = data.withColumn(c + "_f", F.col(c).cast("double"))
    vec_cols = [c + "_f" if c in ("is_weekend", "is_holiday") else c for c in feats]
    data = VectorAssembler(inputCols=vec_cols, outputCol="features").transform(data).persist(StorageLevel.DISK_ONLY)
    train_all = data.filter(F.col("split") == "train")
    test_all = data.filter(F.col("split") == "test")
    trains = {"train_all": train_all, "train_no_gps_jump": train_all.filter(~F.col("has_gps_jump"))}
    tests = {"test_all": test_all, "test_no_gps_jump": test_all.filter(~F.col("has_gps_jump"))}

    # ---------------- kiểm tra leakage tự động (dừng nếu vi phạm)
    seg_train, zone_spd, city_spd = historical_speed_tables(
        spark.read.parquet(str(base / "silver" / "segments")), cut, dt, cfg["routing"]["min_zone_hour_segments"])
    ts = first_row(data.groupBy().agg(
        F.max(F.when(F.col("split") == "train", F.col("start_ts_local"))).alias("max_train"),
        F.min(F.when(F.col("split") == "test", F.col("start_ts_local"))).alias("min_test"),
        F.max(F.when(F.col("split") == "test", F.col("start_ts_local"))).alias("max_test")))
    overlap = train_all.select("trip_uid").join(test_all.select("trip_uid"), "trip_uid").count()
    max_seg = first_row(seg_train.agg(F.max("seg_start_local")))[0]
    checks = {
        "features": feats,
        "features_subset_of_allowed": True,
        "features_contain_forbidden": sorted(set(feats) & FORBIDDEN_FEATURES),
        "train_cutoff_local": cut,
        "max_train_start_local": str(ts["max_train"]),
        "min_test_start_local": str(ts["min_test"]),
        "max_test_start_local": str(ts["max_test"]),
        "time_split_ok": ts["max_train"] < ts["min_test"],
        "trip_uid_in_both_train_and_test": overlap,
        "historical_speed_built_from_segments_before": str(max_seg),
        "historical_speed_train_only_ok": str(max_seg) < cut,
        "hyperparameters_fixed_before_run": {"gbt": GBT_PARAMS, "linear": LR_PARAMS},
        "test_used_for_tuning": False,
        "note_filter": "has_gps_jump chỉ dùng để chọn tập (báo cáo có lọc và không lọc), không là feature",
    }
    assert checks["time_split_ok"] and overlap == 0 and checks["historical_speed_train_only_ok"], checks
    assert not checks["features_contain_forbidden"]
    (ev / f"leakage_checks_{m}.json").write_text(json.dumps(checks, ensure_ascii=False, indent=2), encoding="utf-8")

    # ---------------- huấn luyện và đánh giá
    results, models, timings, importances = {}, {}, {}, {}
    for tname, tr in trains.items():
        mean_label = first_row(tr.agg(F.avg(label)))[0]
        t0 = time.time()
        lr = LinearRegression(featuresCol="features", labelCol=label, predictionCol="pred", **LR_PARAMS).fit(tr)
        timings[f"linear/{tname}"] = round(time.time() - t0, 1)
        t0 = time.time()
        gbt = GBTRegressor(featuresCol="features", labelCol=label, predictionCol="pred", **GBT_PARAMS).fit(tr)
        timings[f"gbt/{tname}"] = round(time.time() - t0, 1)
        models[("linear", tname)], models[("gbt", tname)] = lr, gbt
        importances[tname] = dict(zip(feats, [round(float(x), 4) for x in gbt.featureImportances.toArray()]))
        for ename, te in tests.items():
            key = f"{tname} -> {ename}"
            results[key] = {
                "naive_mean": evaluate(te.withColumn("pred", F.lit(mean_label))),
                "historical_speed": evaluate(historical_speed_baseline(te, zone_spd, city_spd)),
                "linear_regression": evaluate(lr.transform(te)),
                "gbt": evaluate(gbt.transform(te)),
            }
    hb = historical_speed_baseline(test_all, zone_spd, city_spd)
    coverage = first_row(hb.agg(F.avg(F.col("used_zone_speed").cast("double"))))[0]

    out = {
        "mode": m,
        "rows": {"train_all": train_all.count(), "train_no_gps_jump": trains["train_no_gps_jump"].count(),
                 "test_all": test_all.count(), "test_no_gps_jump": tests["test_no_gps_jump"].count()},
        "results": results,
        "gbt_feature_importance": importances,
        "linear_coefficients_train_all": dict(zip(feats, [round(float(x), 5) for x in models[("linear", "train_all")].coefficients])),
        "historical_speed_baseline": {"share_test_trips_with_zone_speed": coverage,
                                      "fallback": "tốc độ toàn thành phố theo giờ × loại ngày (train)"},
        "params": {"gbt": GBT_PARAMS, "linear": LR_PARAMS},
        "fit_seconds": timings,
        "total_seconds": round(time.time() - t_all, 1),
    }
    (ev / f"model_results_{m}.json").write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    # ---------------- lưu model (mục 8)
    if m == "full":
        mdir = ROOT / cfg["paths"]["models"]
        for (alg, tname), model in models.items():
            p = mdir / f"spark_{alg}_{tname}"
            shutil.rmtree(p, ignore_errors=True)
            model.save(str(p))
        meta = {
            "features": feats, "feature_vector_columns": vec_cols, "label": label, "label_unit": "phút",
            "train_cutoff_local": cut, "serving_model": f"spark_{SERVING[0]}_{SERVING[1]}",
            "serving_choice_reason": "chọn trước khi chạy, đúng đề, không dựa vào kết quả test",
            "params": out["params"], "spark_version": spark.version,
        }
        (mdir / "features.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    data.unpersist()
    print(json.dumps({k: {mm: {x: round(v[x], 3) for x in ["mae", "rmse", "r2", "signed_error"]} for mm, v in r.items()}
                      for k, r in results.items()}, indent=2, ensure_ascii=False))
    spark.stop()


if __name__ == "__main__":
    main()
