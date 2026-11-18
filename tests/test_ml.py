"""Hàm đánh giá, baseline tốc độ lịch sử (chỉ từ train) và danh sách feature cho phép."""

import pytest

from core.common import get_spark, load_config  # noqa: E402
from ml.evaluation import check_features, evaluate, historical_speed_baseline, historical_speed_tables  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = get_spark(load_config(), "test-ml")
    yield s
    s.stop()


def test_metrics(spark):
    df = spark.createDataFrame([(10.0, 12.0), (20.0, 18.0), (30.0, 33.0)], ["duration_min", "pred"])
    r = evaluate(df)
    assert r["n"] == 3
    assert abs(r["mae"] - (2 + 2 + 3) / 3) < 1e-9
    assert abs(r["signed_error"] - (2 - 2 + 3) / 3) < 1e-9
    assert abs(r["rmse"] - ((4 + 4 + 9) / 3) ** 0.5) < 1e-9
    assert abs(r["r2"] - (1 - 17 / 200)) < 1e-9


def test_config_features_allowed():
    assert check_features(load_config()["ml"]["features"])
    with pytest.raises(ValueError):
        check_features(["straight_km", "n_points"])        # n_points tính ra nhãn
    with pytest.raises(ValueError):
        check_features(["straight_km", "zone_hour_speed"])  # không nằm trong danh sách cho phép


def test_historical_speed_uses_train_only(spark):
    from datetime import datetime
    rows = [  # (seg_start_local, zone_id, hour, is_weekend, km)
        *[(datetime(2014, 4, 30, 8), 7, 8, False, 0.1)] * 30,     # train: 0,1 km / 15 s = 24 km/h
        *[(datetime(2014, 5, 2, 8), 7, 8, False, 0.4)] * 30,      # test: 96 km/h, KHÔNG được dùng
    ]
    seg = spark.createDataFrame(rows, ["seg_start_local", "zone_id", "hour", "is_weekend", "km"])
    _, zone, city = historical_speed_tables(seg, "2014-05-01", 15, min_segments=20)
    z = zone.collect()
    assert len(z) == 1 and abs(z[0]["zone_speed"] - 24.0) < 1e-9
    trips = spark.createDataFrame([(7, 8, False, 12.0), (99, 8, False, 12.0)],
                                  ["origin_zone", "hour", "is_weekend", "straight_km"])
    p = {r["origin_zone"]: r for r in historical_speed_baseline(trips, zone, city).collect()}
    assert abs(p[7]["pred"] - 30.0) < 1e-9 and p[7]["used_zone_speed"]          # 12 km / 24 km/h = 30 phút
    assert abs(p[99]["pred"] - 30.0) < 1e-9 and not p[99]["used_zone_speed"]     # zone lạ: lùi về tốc độ thành phố


def test_interval_lookup():
    from ml.interval import lookup
    t = {"bin_edges": [-1e9, 8.0, 15.0], "q_low": [-3.0, -4.0, -9.0], "q_high": [4.0, 5.0, 10.0], "min_duration_min": 0.25}
    assert lookup(t, 6.0) == (3.0, 10.0)        # nhóm đầu
    assert lookup(t, 8.0) == (4.0, 13.0)        # đúng biên thì thuộc nhóm sau
    assert lookup(t, 20.0) == (11.0, 30.0)      # nhóm cuối
    assert lookup(t, 1.0) == (0.25, 5.0)        # không âm, tối thiểu 15 giây
