# REST API `POST /estimate` (số đo thật)

Nguồn: `api_latency.json` (`src/checks/api_latency.py`), code `src/serving/api.py`, test `tests/test_api.py`. Ảnh trang thử API: `screenshots/api_docs.png`.

**Thiết kế:** FastAPI bọc **cùng hàm** `estimate()` mà giao diện Streamlit dùng, nên API và giao diện luôn cho cùng kết quả. Chạy bằng service `api` trong Docker Compose (cổng 8000); trang thử tự sinh ở `http://localhost:8000/docs`.

| Endpoint | Mô tả |
|---|---|
| `GET /health` | trạng thái, model đang phục vụ, thứ tự feature đã kiểm tra |
| `POST /estimate` | vào: `origin {lat, lon}`, `destination {lat, lon}`, `departure_time` (giờ Porto, không kèm múi giờ). Ra: `predicted_duration_min`, `route`, `congested_zones`, `live` (4 trường của đề) và `predicted_interval_min` (khoảng 80%) |

**Kiểm tra thứ tự feature khi khởi động (đề mục 8):** API so `models/features.json` với `conf/pipeline.yaml`; lệch thì API không khởi động. Có unit test cho trường hợp lệch. `/health` trả về `feature_order_checked: true`.

## Độ trễ

| | Giá trị |
|---|---|
| Số yêu cầu đo | 50 (50 cặp địa điểm có sẵn, seed 42, 6 khung giờ; 5 lần khởi động không tính) |
| p50 | **6.44 ms** |
| p95 | 11.88 ms |
| max | 15.65 ms |

Độ trễ đo phía client trong mạng Docker, gồm HTTP, kiểm tra input, tính feature, model GBT, tìm tuyến Dijkstra, tra mức tắc và lớp live trong MongoDB.

**Vì sao nhanh:** model dự đoán một vector trực tiếp trên driver bằng `GBTRegressionModel.predict` (khoảng 0,26 ms), không tạo DataFrame và không chạy job Spark (`transform` mất khoảng 262 ms cho cùng kết quả, đã kiểm tra trùng giá trị). Đồ thị zone theo từng giờ được dựng một lần rồi giữ lại.

## F3 qua API

20 chuyến mẫu tập test gửi qua `POST /estimate`: MAE **2.94 phút**, thực tế nằm trong khoảng 80% ở **17/20** chuyến. Kết quả trùng với khi gọi hàm trực tiếp (`F3_end_to_end.md`). Đây chỉ là minh họa; số đánh giá chính thức là MAE trên toàn tập test (`ml/E_model_validation.md`).

Request thiếu trường bắt buộc hoặc có múi giờ bị từ chối với mã **422**.
