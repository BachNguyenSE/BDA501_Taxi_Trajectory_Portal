"""Feature tính lúc phục vụ (Python) phải KHỚP feature lúc huấn luyện (Spark, bảng gold)."""
import pathlib

import pandas as pd
import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
from core.common import load_config  # noqa: E402
from serving.estimate import load_holidays, make_features, zone_of  # noqa: E402

CFG = load_config()
TF = ROOT / "data" / "sample" / "gold" / "trip_features"


@pytest.mark.skipif(not TF.exists(), reason="cần chạy run_pipeline.sh --sample trước")
def test_serving_features_match_training_features():
    df = pd.read_parquet(TF).sample(n=500, random_state=42)
    ts = pd.to_datetime(df["start_ts_local"])
    if ts.dt.tz is not None:
        ts = ts.dt.tz_convert("UTC").dt.tz_localize(None)   # giá trị đồng hồ đã là giờ Lisbon
    hol = load_holidays(CFG)
    for (_, r), t in zip(df.iterrows(), ts):
        f = make_features((r["origin_lat"], r["origin_lon"]), (r["dest_lat"], r["dest_lon"]), t.to_pydatetime(), hol)
        assert abs(f["straight_km"] - r["straight_km"]) < 1e-9
        assert abs(f["bearing_deg"] - r["bearing_deg"]) < 1e-6 or abs(abs(f["bearing_deg"] - r["bearing_deg"]) - 360) < 1e-6
        assert (f["hour"], f["day_of_week"], f["is_weekend"], f["is_holiday"]) == \
               (r["hour"], r["day_of_week"], r["is_weekend"], r["is_holiday"])
        oz = zone_of(r["origin_lon"], r["origin_lat"], CFG["grid"])
        assert (oz is None and pd.isna(r["origin_zone"])) or oz == r["origin_zone"]
