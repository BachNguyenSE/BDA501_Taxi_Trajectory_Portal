# F3. Chạy thật một chuyến từ CSV đến `estimate()` (số đo thật)

Nguồn: `F3_end_to_end.json` (`src/checks/f3_end_to_end.py`). Ảnh chụp giao diện: `evidence/screenshots/`.

**Cách làm:** với mỗi chuyến mẫu trong tập test (05–06/2014), script đọc lại **dòng gốc trong `data/raw/train.csv`** theo `TRIP_ID`. Từ `POLYLINE` nó tự lấy điểm đầu (A), điểm cuối (B), giờ xuất phát (đổi từ epoch UTC sang giờ Lisbon) và thời lượng thật ((số điểm − 1) × 15 giây). Sau đó gọi `estimate(A, B, giờ)`: hàm này tính 6 feature, dự đoán bằng model GBT, tìm tuyến trên đồ thị zone từ MongoDB, gán mức tắc và đọc live. Model phục vụ (`spark_gbt_train_all`) chỉ huấn luyện trên chuyến trước 05/2014, nên đây là **dự đoán ngoài mẫu**.

**Chuyến đầu tiên, phản hồi đầy đủ:** chuyến `1399135956620000324` xuất phát 2014-05-03 17:52:36, thực tế **7.75 phút**, dự đoán **8.16 phút** (sai +0.41 phút). Tuyến 4 zone. Live: chưa có dữ liệu live (zone_speed_live trống).

**20 chuyến mẫu** (chọn ngẫu nhiên seed 42, bỏ chuyến có bước nhảy GPS và chuyến dưới 1 km):

| TRIP_ID | Xuất phát (Lisbon) | Thực tế (phút) | Dự đoán (phút) | Sai (phút) | Zone trên tuyến |
|---|---|---|---|---|---|
| 1399135956620000324 | 2014-05-03 17:52:36 | 7.75 | 8.16 | +0.41 | 4 |
| 1399217372620000084 | 2014-05-04 16:29:32 | 32.75 | 22.13 | -10.62 | 25 |
| 1399226240620000009 | 2014-05-04 18:57:20 | 4.5 | 7.5 | +3.0 | 3 |
| 1399436669620000345 | 2014-05-07 05:24:29 | 19.75 | 15.52 | -4.23 | 21 |
| 1399459583620000314 | 2014-05-07 11:46:23 | 16.0 | 16.26 | +0.26 | 18 |
| 1399923831620000157 | 2014-05-12 20:43:51 | 14.25 | 12.27 | -1.98 | 12 |
| 1400792965620000280 | 2014-05-22 22:09:25 | 4.0 | 6.59 | +2.59 | 3 |
| 1401008173620000672 | 2014-05-25 09:56:13 | 16.0 | 10.01 | -5.99 | 7 |
| 1402051073620000276 | 2014-06-06 11:37:53 | 12.25 | 12.46 | +0.21 | 7 |
| 1402072726620000370 | 2014-06-06 17:38:46 | 21.25 | 20.52 | -0.73 | 13 |
| 1402263306620000508 | 2014-06-08 22:35:06 | 14.0 | 11.53 | -2.47 | 7 |
| 1402400812620000486 | 2014-06-10 12:46:52 | 9.5 | 9.19 | -0.31 | 5 |
| 1402634695620000574 | 2014-06-13 05:44:55 | 10.25 | 8.88 | -1.37 | 7 |
| 1402794617620000351 | 2014-06-15 02:10:17 | 8.25 | 11.08 | +2.83 | 10 |
| 1402913747620000112 | 2014-06-16 11:15:47 | 14.5 | 16.85 | +2.35 | 17 |
| 1402926001620000331 | 2014-06-16 14:40:01 | 17.25 | 19.8 | +2.55 | 16 |
| 1403214774620000328 | 2014-06-19 22:52:54 | 11.5 | 12.44 | +0.94 | 17 |
| 1403580092620000503 | 2014-06-24 04:21:32 | 9.5 | 11.77 | +2.27 | 9 |
| 1403766605620000281 | 2014-06-26 08:10:05 | 14.75 | 18.51 | +3.76 | 17 |
| 1403958872620000456 | 2014-06-28 13:34:32 | 28.25 | 18.27 | -9.98 | 16 |

MAE trên 20 chuyến này: 2.94 phút. **Chỉ để minh họa**: 20 chuyến đã bỏ chuyến nhiễu và chuyến ngắn nên không đại diện. Số đánh giá chính thức là MAE 4,21 phút trên toàn bộ 308.543 chuyến test (`evidence/ml/E_model_validation.md`).

Kiểm tra khớp: A, B, giờ và thời lượng đọc từ CSV gốc trùng với file chuyến mẫu ở cả 20 chuyến: có.

**Khớp feature giữa phục vụ và huấn luyện:** `tests/test_estimate.py` so 500 chuyến. Feature tính lúc phục vụ (Python) trùng với bảng gold (Spark) ở cả 6 feature và zone xuất phát.
