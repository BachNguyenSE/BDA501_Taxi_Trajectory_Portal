"""REST API: hợp đồng request/response và kiểm tra thứ tự feature khi khởi động (không cần MongoDB hay Spark)."""
import pytest
from fastapi.testclient import TestClient

from core.common import load_config
from serving.api import check_feature_order, create_app

CFG = load_config()


class FakeEstimator:
    def __init__(self, features=None):
        self.meta = {"features": features or CFG["ml"]["features"], "serving_model": "fake"}
        self.interval = {"x": 1}
        self.calls = []

    def estimate(self, origin, destination, departure_time):
        self.calls.append((origin, destination, departure_time))
        return {"predicted_duration_min": 12.5, "predicted_interval_min": [9.0, 17.0],
                "route": [{"zone_id": 1440, "level": "đông"}], "congested_zones": [],
                "live": {"available": False, "slow_zones": []}, "debug": {}}


BODY = {"origin": {"lat": 41.2378, "lon": -8.6707}, "destination": {"lat": 41.1456, "lon": -8.6105},
        "departure_time": "2014-05-14T17:30:00"}


def test_estimate_returns_required_fields():
    est = FakeEstimator()
    with TestClient(create_app(est)) as c:
        r = c.post("/estimate", json=BODY)
    assert r.status_code == 200
    body = r.json()
    for k in ["predicted_duration_min", "route", "congested_zones", "live"]:   # 4 trường của đề
        assert k in body
    assert body["predicted_interval_min"] == [9.0, 17.0]
    (o, d, t), = est.calls
    assert o == (41.2378, -8.6707) and d == (41.1456, -8.6105) and t.hour == 17 and t.tzinfo is None


def test_rejects_bad_input():
    with TestClient(create_app(FakeEstimator())) as c:
        assert c.post("/estimate", json={**BODY, "departure_time": "2014-05-14T17:30:00+01:00"}).status_code == 422
        assert c.post("/estimate", json={**BODY, "origin": {"lat": 200, "lon": 0}}).status_code == 422
        assert c.post("/estimate", json={"origin": BODY["origin"]}).status_code == 422


def test_health_reports_checked_feature_order():
    with TestClient(create_app(FakeEstimator())) as c:
        h = c.get("/health").json()
    assert h["feature_order_checked"] and h["features"] == CFG["ml"]["features"]


def test_startup_fails_on_feature_order_mismatch():
    wrong = list(reversed(CFG["ml"]["features"]))
    with pytest.raises(RuntimeError):
        check_feature_order({"features": wrong}, CFG)
    with pytest.raises(RuntimeError):
        with TestClient(create_app(FakeEstimator(wrong))):
            pass
