"""Hợp đồng của bronze: mọi dòng được giữ, mỗi dòng đúng một reject_reason hoặc null."""

import pytest


from batch.bronze import build_bronze  # noqa: E402
from core.common import get_spark, load_config  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    cfg = load_config()
    s = get_spark(cfg, "test-bronze")
    yield s
    s.stop()


def pl(n):
    return "[" + ",".join("[-8.6,41.1]" for _ in range(n)) + "]"


COLS = ["TRIP_ID", "CALL_TYPE", "TAXI_ID", "TIMESTAMP", "MISSING_DATA", "POLYLINE"]


def run(spark, rows):
    raw = spark.createDataFrame([(f"t{i}", "A", "1", "1372636858", m, p) for i, (m, p) in enumerate(rows)], COLS)
    out = build_bronze(raw, load_config())
    return {r["trip_id"]: r for r in out.collect()}


def test_reject_reasons_and_precedence(spark):
    r = run(spark, [
        ("False", pl(10)),          # t0 hợp lệ
        ("True", pl(10)),           # t1 missing_data
        ("True", "[[1,"),           # t2 missing_data thắng bad_json
        ("False", "[[1,"),          # t3 bad_json
        ("False", "[]"),            # t4 too_few_points (0 điểm)
        ("False", pl(1)),           # t5 too_few_points (1 điểm)
        ("False", pl(721)),         # t6 đúng 180 phút (720 * 15 s): hợp lệ
        ("False", pl(722)),         # t7 180 phút 15 giây: too_long
    ])
    assert len(r) == 8, "bronze phải giữ mọi dòng"
    assert r["t0"]["reject_reason"] is None and r["t0"]["duration_s"] == 9 * 15
    assert r["t1"]["reject_reason"] == "missing_data"
    assert r["t2"]["reject_reason"] == "missing_data"
    assert r["t3"]["reject_reason"] == "bad_json"
    assert r["t4"]["reject_reason"] == "too_few_points"
    assert r["t5"]["reject_reason"] == "too_few_points"
    assert r["t6"]["reject_reason"] is None and r["t6"]["duration_s"] == 720 * 15
    assert r["t7"]["reject_reason"] == "too_long"


def test_lon_lat_order_and_local_time(spark):
    r = run(spark, [("False", "[[-8.618643,41.141412],[-8.618499,41.141376]]")])["t0"]
    assert r["polyline"][0] == [-8.618643, 41.141412]  # lon trước, lat sau
    # epoch 1372636858 = 2013-07-01 00:00:58 UTC = 01:00:58 giờ Lisbon (UTC+1, giờ mùa hè)
    assert str(r["start_ts_local"]) == "2013-07-01 01:00:58"
