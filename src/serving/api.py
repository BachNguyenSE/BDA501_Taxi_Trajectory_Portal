"""REST API (mục 10): POST /estimate bọc hàm estimate() dùng chung với giao diện Streamlit.

Chạy: docker compose up -d api   ->  http://localhost:8000/docs (trang thử API tự sinh)
  GET  /health     trạng thái, model đang phục vụ, thứ tự feature đã kiểm tra
  POST /estimate   {origin, destination, departure_time}
                   -> predicted_duration_min, route, congested_zones, live (4 trường của đề)
                      + predicted_interval_min (khoảng 80%, mở rộng đã duyệt)
Khi khởi động, API KIỂM TRA thứ tự feature của model (models/features.json) khớp conf/pipeline.yaml (đề mục 8);
lệch thì API không khởi động, tránh dự đoán sai mà không báo lỗi.
departure_time là giờ địa phương Porto (Europe/Lisbon), không kèm múi giờ, ví dụ "2014-05-14T17:30:00".
"""
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator

from core.common import load_config


class Point(BaseModel):
    lat: float = Field(..., ge=-90, le=90, examples=[41.2378])
    lon: float = Field(..., ge=-180, le=180, examples=[-8.6707])


class EstimateRequest(BaseModel):
    origin: Point
    destination: Point
    departure_time: datetime = Field(..., description="Giờ địa phương Porto, không kèm múi giờ",
                                     examples=["2014-05-14T17:30:00"])

    @field_validator("departure_time")
    @classmethod
    def local_naive(cls, v: datetime):
        if v.tzinfo is not None:
            raise ValueError("departure_time phải là giờ địa phương Porto, không kèm múi giờ (ví dụ 2014-05-14T17:30:00)")
        return v


class EstimateResponse(BaseModel):
    predicted_duration_min: float
    predicted_interval_min: list[float] | None
    route: list[dict]
    congested_zones: list[int]
    live: dict
    debug: dict


def check_feature_order(meta: dict, cfg: dict):
    """Đề mục 8: API kiểm tra thứ tự feature khi khởi động."""
    if meta["features"] != cfg["ml"]["features"]:
        raise RuntimeError(f"thứ tự feature của model {meta['features']} khác cấu hình {cfg['ml']['features']}")
    return meta["features"]


def create_app(estimator=None) -> FastAPI:
    """estimator=None: tạo Estimator thật (MongoDB + model Spark). Test truyền estimator giả."""
    state = {}

    @asynccontextmanager
    async def lifespan(app):
        from serving.estimate import Estimator
        cfg = load_config()
        est = estimator or Estimator(cfg)
        state["features"] = check_feature_order(est.meta, cfg)
        if estimator is None:
            est.model()   # khởi động Spark và nạp model một lần, trước khi nhận yêu cầu
        state["est"] = est
        yield

    app = FastAPI(title="Porto taxi ETA API", version="1.0", lifespan=lifespan,
                  description="Known-destination ETA cho taxi Porto (BDA501, Card F). Dữ liệu live là bản phát lại, không phải real-time.")

    @app.get("/health")
    def health():
        est = state["est"]
        return {"status": "ok", "model": est.meta["serving_model"], "features": state["features"],
                "feature_order_checked": True, "interval": est.interval is not None}

    @app.post("/estimate", response_model=EstimateResponse)
    def estimate(req: EstimateRequest):
        try:
            res = state["est"].estimate((req.origin.lat, req.origin.lon), (req.destination.lat, req.destination.lon),
                                        req.departure_time)
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))
        return res

    return app


app = create_app()
