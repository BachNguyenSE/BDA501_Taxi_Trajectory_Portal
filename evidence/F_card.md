# F. Card F: Porto Taxi GPS Trip Duration & ETA

## F1. Leakage: feature có trước khi xuất phát hay không

| Feature | Lấy từ | Có trước khi xuất phát? | Dùng |
|---|---|---|---|
| `straight_km` | haversine(A, B), A và B do khách nhập | Có | Có |
| `bearing_deg` | hướng A → B | Có | Có |
| `hour` | giờ Lisbon lúc xuất phát | Có | Có |
| `day_of_week`, `is_weekend` | ngày Lisbon | Có | Có |
| `is_holiday` | `holidays.csv` (lịch công bố trước) | Có | Có |
| số điểm GPS, độ dài polyline, tốc độ thực tế của chuyến | chính quỹ đạo (nhãn = (số điểm − 1) × 15 giây) | **Không** | Cấm |
| `zone_hour_speed` tính trên toàn bộ dữ liệu | gồm cả giai đoạn test | **Không** | Cấm; baseline dùng bản tính từ train |
| `has_gps_jump` | chính quỹ đạo | Không | Chỉ để chọn tập báo cáo, không là feature |

Kiểm tra tự động (`ml/leakage_checks_full.json`, script dừng nếu vi phạm): train muộn nhất 2014-04-30 23:59:47, test sớm nhất 2014-05-01 00:00:52, `trip_uid` ở cả hai tập: 0, đoạn GPS muộn nhất dùng cho tốc độ lịch sử: 2014-04-30 23:59:59. Tham số model cố định trước khi chạy. Chi tiết: `docs/DATA_LEAKAGE.md`.

## F2. Baseline theo thời gian (temporal hold-out)

Train trước 01/05/2014, test 05–06/2014 (308,543 chuyến):

| Model | MAE (phút) | RMSE | R² | Signed error |
|---|---|---|---|---|
| Naive (trung bình train) | 5.545 | 9.414 | -0.000 | -0.042 |
| Tốc độ lịch sử | 6.338 | 15.789 | -1.813 | -0.536 |
| Linear Regression | 4.566 | 8.560 | 0.173 | +0.034 |
| GBT Regressor | 4.213 | 8.307 | 0.221 | +0.055 |

Chi tiết và các tổ hợp có lọc nhãn nhiễu: `ml/E_model_validation.md`.

## F3. Chạy thật một chuyến từ CSV đến `estimate()`

Chuyến `1399135956620000324` (xuất phát 2014-05-03 17:52:36, giờ Lisbon) được đọc lại từ dòng gốc trong `train.csv`. Thời lượng thực tế **7.75 phút**, model dự đoán **8.16 phút**. Có 20 chuyến mẫu ngoài mẫu, tất cả khớp với dòng gốc. Chi tiết: `serving/F3_end_to_end.md`; ảnh: `screenshots/ui_sample_trip.jpg`.
