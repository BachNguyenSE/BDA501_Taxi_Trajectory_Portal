"""Hợp đồng của silver và gold theo các quy tắc đề đặt ra (mục 5)."""
import json
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]

from batch.bronze import build_bronze  # noqa: E402
from core.common import get_spark, load_config  # noqa: E402
from batch.gold import build_edges, build_trip_features, build_zone_hour_speed  # noqa: E402
from batch.silver import build_segments, build_trips  # noqa: E402

CFG = load_config()
COLS = ["TRIP_ID", "CALL_TYPE", "TAXI_ID", "TIMESTAMP", "MISSING_DATA", "POLYLINE"]


@pytest.fixture(scope="module")
def spark():
    s = get_spark(CFG, "test-silver-gold")
    yield s
    s.stop()


def trips_of(spark, rows):
    """rows: list of (trip_id, epoch, [[lon, lat], ...])"""
    raw = spark.createDataFrame([(t, "A", "1", str(e), "False", json.dumps(p)) for t, e, p in rows], COLS)
    return build_trips(build_bronze(raw, CFG), CFG)


def segs(spark, rows):
    out = build_segments(trips_of(spark, rows), CFG).collect()
    return sorted(out, key=lambda r: (r["trip_uid"], r["seq"]))


def test_no_bbox_filter_before_lag(spark):
    # A trong bbox, B ngoài bbox, C trong bbox. Nếu lọc bbox trước lag sẽ sinh đoạn giả A-C.
    a, b_out, c = [-8.61, 41.15], [-9.00, 41.15], [-8.611, 41.15]
    assert segs(spark, [("t", 1372636858, [a, b_out, c])]) == []


def test_speed_filter(spark):
    # 0.01 độ lat ~ 1.11 km trong 15 s ~ 267 km/h: bỏ; 0.002 độ ~ 0.22 km ~ 53 km/h: giữ
    out = segs(spark, [("t", 1372636858, [[-8.61, 41.150], [-8.61, 41.160], [-8.61, 41.162]])])
    assert [r["seq"] for r in out] == [2]
    assert 50 < out[0]["speed_kmh"] < 56


def test_midpoint_zone_and_local_start_hour(spark):
    # bắt đầu 00:59:58 UTC = 01:59:58 giờ Lisbon; đoạn 1 bắt đầu 01:59:58 (giờ 1), đoạn 2 bắt đầu 02:00:13 (giờ 2)
    lat = 41.1525                         # gy = 20
    p = [[-8.6014, lat], [-8.5966, lat], [-8.5960, lat]]   # gx 39 -> 40 -> 40
    out = segs(spark, [("t", 1372636858 + 3540, p)])
    assert [r["hour"] for r in out] == [1, 2]
    s1 = out[0]
    assert (s1["zone_from"], s1["zone_to"], s1["zone_id"]) == (20 * 70 + 39, 20 * 70 + 40, 20 * 70 + 40)


def test_zone_hour_speed_is_sum_not_mean_of_means(spark):
    seg = build_segments(trips_of(spark, [
        ("t1", 1372636858, [[-8.61, 41.150], [-8.61, 41.151]]),                      # 1 đoạn
        ("t2", 1372636858, [[-8.61, 41.150], [-8.61, 41.150], [-8.61, 41.150]]),     # 2 đoạn đứng yên
    ]), CFG)
    r = build_zone_hour_speed(seg, CFG).collect()
    assert len(r) == 1 and r[0]["n_segments"] == 3 and r[0]["total_s"] == 45
    # tổng km / tổng giờ: 0.111 km / 45 s, không phải trung bình của (26.7, 0, 0)
    assert abs(r[0]["avg_speed_kmh"] - r[0]["total_km"] / (45 / 3600)) < 1e-9


def test_edges_only_adjacent_zones(spark):
    lat = 41.1525
    seg = build_segments(trips_of(spark, [("t", 1372636858, [[-8.6014, lat], [-8.5966, lat]])]), CFG)
    e = build_edges(seg, CFG).collect()
    assert [(x["zone_from"], x["zone_to"], x["n_transitions"]) for x in e] == [(1439, 1440, 1)]
    assert e[0]["travel_time_s"] > 0


def test_trip_features_split_holiday_bearing(spark):
    hol = spark.read.option("header", True).csv(str(ROOT / "data" / "raw" / "holidays.csv"))
    north = [[-8.61, 41.15], [-8.61, 41.151]]
    east = [[-8.61, 41.15], [-8.609, 41.15]]
    tf = build_trip_features(trips_of(spark, [
        ("train_last", 1398898800 - 1, north),   # 2014-04-30 23:59:59 giờ Lisbon
        ("test_first", 1398898800, east),        # 2014-05-01 00:00:00 giờ Lisbon, ngày lễ 1/5
    ]), hol, CFG)
    r = {x["trip_id"]: x for x in tf.collect()}
    assert r["train_last"]["split"] == "train" and r["train_last"]["is_holiday"] is False
    assert r["test_first"]["split"] == "test" and r["test_first"]["is_holiday"] is True
    assert r["test_first"]["hour"] == 0 and r["test_first"]["day_of_week"] == 5   # Thứ năm
    assert abs(r["train_last"]["bearing_deg"] - 0) < 0.5 or abs(r["train_last"]["bearing_deg"] - 360) < 0.5
    assert abs(r["test_first"]["bearing_deg"] - 90) < 0.5
    assert r["test_first"]["duration_min"] == 0.25


def test_shared_trip_id_does_not_mix_trips(spark):
    # Hai chuyến khác nhau, cùng TRIP_ID (cùng xe, cùng giây): không được ghép điểm chéo nhau
    a = [[-8.61, 41.150], [-8.61, 41.151], [-8.61, 41.152]]
    b = [[-8.58, 41.170], [-8.58, 41.171]]
    out = segs(spark, [("same", 1372636858, a), ("same", 1372636858, b)])
    assert len(out) == (3 - 1) + (2 - 1)
    assert len({r["trip_uid"] for r in out}) == 2
    assert all(r["speed_kmh"] < 30 for r in out)   # đoạn ghép chéo sẽ có tốc độ hàng trăm km/h


def test_repeated_identical_trip_kept_once(spark):
    p = [[-8.61, 41.150], [-8.61, 41.151], [-8.61, 41.152]]
    out = segs(spark, [("dup", 1372636858, p), ("dup", 1372636858, p), ("dup", 1372636858, p)])
    assert len(out) == len(p) - 1
