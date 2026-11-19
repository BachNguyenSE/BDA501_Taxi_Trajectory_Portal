"""Job: thử FEATURE THỜI GIAN THEO ĐỒ THỊ TUYẾN cho model ETA (đã hứa ở Session 9).

Mục trong sơ đồ : 7 (Spark ML).
Làm gì          : thêm graph_time_min, graph_km, graph_found (src/ml/graph_features.py) vào 6 feature của đề,
                  rồi QUYẾT ĐỊNH giữ hay bỏ dựa trên tháng validation, không nhìn tập test.
Quy trình (chốt trước khi chạy):
  1. Chia các tuần thành K=5 fold theo (số tuần kể từ 01/07/2013) mod 5.
  2. Giai đoạn VALIDATION: chỉ dùng đoạn GPS trước 01/04/2014.
       - chuyến 07/2013-03/2014 (huấn luyện): feature từ đồ thị của 4 fold tuần KHÁC (out-of-fold)
       - chuyến 04/2014 (validation): feature từ đồ thị của toàn bộ đoạn trước 01/04/2014
     Huấn luyện GBT 6 feature và GBT 9 feature trên 07/2013-03/2014, so MAE trên 04/2014.
     QUY TẮC: giữ feature đồ thị nếu MAE của GBT trên 04/2014 GIẢM.
  3. Chỉ khi giữ: giai đoạn TEST, chỉ dùng đoạn GPS trước 01/05/2014.
       - chuyến train (trước 05/2014): out-of-fold như trên
       - chuyến test 05-06/2014: đồ thị của toàn bộ đoạn trước 01/05/2014
     Huấn luyện lại trên toàn bộ train, đo trên test MỘT lần, lưu model.
Tham số GBT giống model chính (job_train_eta.GBT_PARAMS), không tinh chỉnh.
Đọc            : data/silver/segments, data/gold/trip_features, data/gold/zone_graph_edges (chỉ lấy khoảng cách tâm-tâm)
Ghi            : data/gold/trip_graph_features_{validation,test}.parquet, evidence/ml/graph_features.json,
                 models/spark_gbt_graph_train_all + models/features_graph.json (chỉ khi giữ)
Chạy           : docker compose run --rm --no-deps spark python src/ml/job_graph_features.py
"""
import json
import shutil
import time

import pandas as pd
from pyspark import StorageLevel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.sql import functions as F

from core.common import ROOT, first_row, get_spark, load_config
from core.geo import zone_gx, zone_gy
from ml.evaluation import evaluate
from ml.graph_features import GRAPH_FEATURES, all_folds, leave_fold_out, route_features
from ml.job_train_eta import GBT_PARAMS, LR_PARAMS

K = 5
EPOCH = "2013-07-01"
ZH_KEYS, ZH_SUMS = ["zone_id", "hour", "is_weekend"], ["total_km", "total_s", "n_segments"]
ED_KEYS, ED_SUMS = ["zone_from", "zone_to"], ["n_transitions"]


def fold_col(ts_col):
    return (F.floor(F.datediff(ts_col, F.lit(EPOCH)) / 7) % K).cast("int")


def aggregates(seg, cut, cfg):
    """Tổng hợp theo fold tuần, CHỈ từ đoạn bắt đầu trước mốc cut (giờ Lisbon)."""
    grid, dt = cfg["grid"], cfg["cleaning"]["gps_interval_s"]
    s = seg.filter(F.col("seg_start_local") < F.lit(cut).cast("timestamp")).withColumn("fold", fold_col(F.col("seg_start_local")))
    zh = (s.groupBy("fold", *ZH_KEYS).agg(F.sum("km").alias("total_km"), F.count("*").alias("n_segments"))
          .withColumn("total_s", F.col("n_segments") * dt)).toPandas()
    f, t = F.col("zone_from"), F.col("zone_to")
    cheb = F.greatest(F.abs(zone_gx(f, grid) - zone_gx(t, grid)), F.abs(zone_gy(f, grid) - zone_gy(t, grid)))
    ed = s.filter((f != t) & (cheb == 1)).groupBy("fold", *ED_KEYS).agg(F.count("*").alias("n_transitions")).toPandas()
    max_seg = first_row(s.agg(F.max("seg_start_local")))[0]
    return zh[["fold", *ZH_KEYS, *ZH_SUMS]], ed, str(max_seg)


def features(trips, zh, ed, center, cfg, oof):
    parts = []
    if oof:
        for k in range(K):
            rows = trips[trips["fold"] == k]
            z = leave_fold_out(zh, ZH_KEYS, ZH_SUMS, k)
            e = leave_fold_out(ed, ED_KEYS, ED_SUMS, k).merge(center, on=ED_KEYS)
            parts.append(route_features(rows, z, e, cfg))
    else:
        parts.append(route_features(trips, all_folds(zh, ZH_KEYS, ZH_SUMS),
                                    all_folds(ed, ED_KEYS, ED_SUMS).merge(center, on=ED_KEYS), cfg))
    return pd.concat(parts).loc[trips.index]


def main():
    cfg = load_config()
    spark = get_spark(cfg, "job-graph-features")
    spark.sparkContext.setLogLevel("WARN")
    label = cfg["ml"]["label"]
    cut, vfrom = cfg["split"]["train_before"], cfg["split"]["validation_from"]
    meta = json.loads((ROOT / cfg["paths"]["models"] / "features.json").read_text(encoding="utf-8"))
    base_cols = meta["feature_vector_columns"]
    gold = ROOT / cfg["paths"]["gold"]
    t0 = time.time()

    seg = (spark.read.parquet(str(ROOT / cfg["paths"]["silver"] / "segments"))
           .select("seg_start_local", "zone_id", "hour", "is_weekend", "km", "zone_from", "zone_to")
           .persist(StorageLevel.DISK_ONLY))
    center = pd.read_parquet(gold / "zone_graph_edges", columns=["zone_from", "zone_to", "center_km"])
    tf = (spark.read.parquet(str(gold / "trip_features"))
          .filter(F.col("split").isin("train", "test"))
          .select("trip_uid", "split", "start_ts_local", "origin_zone", "dest_zone", "hour", "is_weekend",
                  fold_col(F.col("start_ts_local")).alias("fold"))
          .toPandas())
    ts = pd.to_datetime(tf["start_ts_local"])
    if ts.dt.tz is not None:
        ts = ts.dt.tz_convert("UTC").dt.tz_localize(None)
    tf["ts"] = ts

    out = {"K_week_folds": K, "rule": "giữ feature đồ thị nếu MAE của GBT trên 04/2014 giảm", "gbt_params": GBT_PARAMS}

    # ---------------- giai đoạn VALIDATION: chỉ đoạn trước 01/04/2014
    zh_v, ed_v, max_v = aggregates(seg, vfrom, cfg)
    fit_v = tf[tf["ts"] < pd.Timestamp(vfrom)]
    val_v = tf[(tf["ts"] >= pd.Timestamp(vfrom)) & (tf["ts"] < pd.Timestamp(cut))]
    feat_v = pd.concat([features(fit_v, zh_v, ed_v, center, cfg, oof=True).assign(role="fit"),
                        features(val_v, zh_v, ed_v, center, cfg, oof=False).assign(role="eval")])
    feat_v["trip_uid"] = tf.loc[feat_v.index, "trip_uid"]
    pv = gold / "trip_graph_features_validation.parquet"
    feat_v.to_parquet(pv, index=False)
    out["validation"] = {"segments_used_before": vfrom, "max_segment_time_used": max_v,
                         "fit_trips": len(fit_v), "eval_trips": len(val_v),
                         "graph_found_share_fit": float(feat_v.loc[feat_v["role"] == "fit", "graph_found"].mean()),
                         "graph_found_share_eval": float(feat_v.loc[feat_v["role"] == "eval", "graph_found"].mean())}

    def ml_frame(path):
        g = spark.read.parquet(str(path))
        d = (spark.read.parquet(str(gold / "trip_features")).join(g, "trip_uid")
             .withColumn("is_weekend_f", F.col("is_weekend").cast("double"))
             .withColumn("is_holiday_f", F.col("is_holiday").cast("double")))
        d = VectorAssembler(inputCols=base_cols, outputCol="f_base").transform(d)
        d = VectorAssembler(inputCols=base_cols + GRAPH_FEATURES, outputCol="f_graph").transform(d)
        return d.persist(StorageLevel.DISK_ONLY)

    def fit_eval(fit, ev, tag):
        res, models = {}, {}
        for name, col in [("base_6_features", "f_base"), ("with_graph_features", "f_graph")]:
            gbt = GBTRegressor(featuresCol=col, labelCol=label, predictionCol="pred", **GBT_PARAMS).fit(fit)
            lr = LinearRegression(featuresCol=col, labelCol=label, predictionCol="pred", **LR_PARAMS).fit(fit)
            res[name] = {"gbt": evaluate(gbt.transform(ev)), "linear": evaluate(lr.transform(ev))}
            models[name] = gbt
            if name == "with_graph_features":
                res[name]["gbt_feature_importance"] = dict(zip(base_cols + GRAPH_FEATURES,
                                                               [round(float(x), 4) for x in gbt.featureImportances.toArray()]))
        return res, models

    dv = ml_frame(pv)
    res_v, _ = fit_eval(dv.filter(F.col("role") == "fit"), dv.filter(F.col("role") == "eval"), "validation")
    out["validation"]["results"] = res_v
    mae_b, mae_g = res_v["base_6_features"]["gbt"]["mae"], res_v["with_graph_features"]["gbt"]["mae"]
    keep = mae_g < mae_b
    out["decision"] = {"gbt_mae_validation_base": mae_b, "gbt_mae_validation_with_graph": mae_g, "keep_graph_features": keep}
    print("VALIDATION 04/2014: GBT MAE base", round(mae_b, 4), "| with graph", round(mae_g, 4), "| keep:", keep, flush=True)

    # ---------------- giai đoạn TEST (chỉ khi giữ)
    if keep:
        zh_t, ed_t, max_t = aggregates(seg, cut, cfg)
        fit_t, test_t = tf[tf["split"] == "train"], tf[tf["split"] == "test"]
        feat_t = pd.concat([features(fit_t, zh_t, ed_t, center, cfg, oof=True).assign(role="fit"),
                            features(test_t, zh_t, ed_t, center, cfg, oof=False).assign(role="eval")])
        feat_t["trip_uid"] = tf.loc[feat_t.index, "trip_uid"]
        pt = gold / "trip_graph_features_test.parquet"
        feat_t.to_parquet(pt, index=False)
        dt_ = ml_frame(pt)
        res_t, models = fit_eval(dt_.filter(F.col("role") == "fit"), dt_.filter(F.col("role") == "eval"), "test")
        out["test"] = {"segments_used_before": cut, "max_segment_time_used": max_t,
                       "fit_trips": len(fit_t), "eval_trips": len(test_t),
                       "graph_found_share_eval": float(feat_t.loc[feat_t["role"] == "eval", "graph_found"].mean()),
                       "results": res_t}
        mdir = ROOT / cfg["paths"]["models"]
        p = mdir / "spark_gbt_graph_train_all"
        shutil.rmtree(p, ignore_errors=True)
        models["with_graph_features"].save(str(p))
        (mdir / "features_graph.json").write_text(json.dumps({
            "features": cfg["ml"]["features"] + GRAPH_FEATURES, "feature_vector_columns": base_cols + GRAPH_FEATURES,
            "label": label, "train_cutoff_local": cut, "model": "spark_gbt_graph_train_all",
            "graph_built_from_segments_before": cut, "note": "đồ thị tính feature lúc phục vụ phải xây từ đoạn trước mốc này"},
            ensure_ascii=False, indent=2), encoding="utf-8")
        print("TEST 05-06/2014: GBT MAE base", round(res_t["base_6_features"]["gbt"]["mae"], 4),
              "| with graph", round(res_t["with_graph_features"]["gbt"]["mae"], 4), flush=True)
    else:
        out["test"] = "không chạy: feature đồ thị không cải thiện trên validation"

    out["seconds"] = round(time.time() - t0, 1)
    ev = ROOT / "evidence" / "ml"
    (ev / "graph_features.json").write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    seg.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
