# E. Kiểm tra model: temporal hold-out cùng 2 baseline (số đo thật)

Chạy bằng `python src/ml/job_train_eta.py --mode full` (sample trước, full sau). Nguồn: `model_results_full.json`, `leakage_checks_full.json`. Unit test: 25 test qua (`evidence/validation/phase4_tests.txt`).

## Thiết lập

- **Bài toán:** known-destination ETA. Dự đoán `duration_min` = (số điểm GPS − 1) × 15 giây / 60.
- **Feature (đúng 6 feature của đề):** `straight_km`, `bearing_deg`, `hour`, `day_of_week`, `is_weekend`, `is_holiday`. Tất cả đều biết trước khi xuất phát (bảng F1 trong `docs/DATA_LEAKAGE.md`).
- **Chia theo thời gian (giờ Lisbon):** train là chuyến xuất phát trước 01/05/2014 (1,364,636 chuyến), test là 05–06/2014 (308,543 chuyến). Không dùng random split.
- **Thuật toán:** Spark ML `LinearRegression` (baseline tuyến tính) và `GBTRegressor`. Tham số **cố định trước khi chạy**, không tinh chỉnh: GBT `maxIter=50, maxDepth=6, stepSize=0.1, maxBins=32, seed=42`; Linear không regularization.
- **Hai baseline bắt buộc:** naive (luôn đoán trung bình của train), và tốc độ lịch sử (khoảng cách đường thẳng A–B chia tốc độ lịch sử của zone xuất phát tại giờ và loại ngày đó). Tốc độ lịch sử **chỉ tính từ đoạn GPS trước 01/05/2014**; ô zone × giờ có dưới 20 đoạn thì lùi về tốc độ toàn thành phố. 99.85% chuyến test có tốc độ zone.
- **Nhãn nhiễu (đã duyệt báo cáo cả hai):** chuyến có ít nhất một bước nhảy GPS > 150 km/h. Có 2 tập train (đủ, và bỏ chuyến có bước nhảy: 1,220,700) × 2 tập test (đủ, và bỏ chuyến có bước nhảy: 278,057). Cờ này chỉ dùng để chọn tập, **không phải feature**.
- **Metric:** MAE, RMSE, R², signed error = trung bình (dự đoán − thực tế). Signed error dương nghĩa là model đoán dài hơn thực tế.

## Kết quả chính: train đủ → test đủ

| Model | MAE (phút) | RMSE (phút) | R² | Signed error (phút) |
|---|---|---|---|---|
| Naive (luôn đoán trung bình train) | 5.545 | 9.414 | -0.000 | -0.042 |
| Tốc độ lịch sử (đường thẳng / tốc độ zone × giờ) | 6.338 | 15.789 | -1.813 | -0.536 |
| Linear Regression | 4.566 | 8.560 | 0.173 | +0.034 |
| GBT Regressor | 4.213 | 8.307 | 0.221 | +0.055 |

## Có lọc chuyến nhảy GPS

**Train đủ → test bỏ chuyến nhảy GPS**

| Model | MAE (phút) | RMSE (phút) | R² | Signed error (phút) |
|---|---|---|---|---|
| Naive (luôn đoán trung bình train) | 5.363 | 8.745 | -0.000 | +0.167 |
| Tốc độ lịch sử (đường thẳng / tốc độ zone × giờ) | 5.881 | 13.478 | -1.377 | -1.014 |
| Linear Regression | 4.400 | 7.912 | 0.181 | +0.118 |
| GBT Regressor | 4.032 | 7.685 | 0.227 | +0.128 |

**Train bỏ chuyến nhảy GPS → test đủ**

| Model | MAE (phút) | RMSE (phút) | R² | Signed error (phút) |
|---|---|---|---|---|
| Naive (luôn đoán trung bình train) | 5.506 | 9.417 | -0.001 | -0.217 |
| Tốc độ lịch sử (đường thẳng / tốc độ zone × giờ) | 6.338 | 15.789 | -1.813 | -0.536 |
| Linear Regression | 4.487 | 8.621 | 0.161 | +0.053 |
| GBT Regressor | 4.205 | 8.337 | 0.216 | +0.008 |

**Train bỏ → test bỏ**

| Model | MAE (phút) | RMSE (phút) | R² | Signed error (phút) |
|---|---|---|---|---|
| Naive (luôn đoán trung bình train) | 5.322 | 8.743 | -0.000 | -0.008 |
| Tốc độ lịch sử (đường thẳng / tốc độ zone × giờ) | 5.881 | 13.478 | -1.377 | -1.014 |
| Linear Regression | 4.304 | 7.871 | 0.189 | +0.103 |
| GBT Regressor | 4.006 | 7.686 | 0.227 | +0.069 |

## Đọc kết quả

- **GBT tốt nhất ở cả 4 tổ hợp.** MAE 4.21 phút, so với naive 5.55 phút (giảm 24%) và Linear 4.57 phút. R² chỉ khoảng 0.22: chỉ với A, B và thời điểm thì phần lớn biến động thời gian chuyến không giải thích được (tuyến thực tế, chờ khách, kẹt bất ngờ). Đây là giới hạn của bài toán chỉ dùng thông tin trước khi xuất phát.
- **Signed error gần 0** (dưới 0,2 phút) với Linear và GBT: model không lệch hệ thống về một phía.
- **Baseline tốc độ lịch sử tệ hơn cả naive** (MAE 6.34, R² -1.81). Em giữ nguyên kết quả này, không chỉnh baseline cho đẹp. Giả thuyết, **chưa đo**: đường thẳng ngắn hơn đường thật, và tốc độ zone gồm cả lúc xe đứng chờ (như zone 2685 gần sân bay chỉ 1,65 km/h) nên có chuyến bị đoán quá dài. RMSE lớn (15.8) phù hợp với giả thuyết có một số dự đoán sai rất xa.
- **Lọc chuyến nhảy GPS:** bỏ chúng khỏi tập test làm MAE giảm (4.213 xuống 4.032), vì nhãn của các chuyến này kém tin cậy. Bỏ chúng khỏi tập train gần như không đổi kết quả (4.213 so với 4.205). Số dùng để báo cáo chính là dòng **train đủ → test đủ**, không lọc, đúng như đề.
- **Mức quan trọng của feature (GBT, train đủ):** straight_km 0.55, hour 0.24, bearing_deg 0.14, day_of_week 0.04, is_weekend 0.03, is_holiday 0.009.

## Kiểm tra leakage tự động (script dừng nếu vi phạm)

| Kiểm tra | Kết quả |
|---|---|
| Feature thuộc danh sách cho phép, không có feature cấm | Đạt (straight_km, bearing_deg, hour, day_of_week, is_weekend, is_holiday) |
| Chuyến train xuất phát muộn nhất (giờ Lisbon) | 2014-04-30 23:59:47 |
| Chuyến test xuất phát sớm nhất | 2014-05-01 00:00:52 |
| Không chồng thời gian giữa train và test | Đạt |
| `trip_uid` xuất hiện ở cả train và test | 0 |
| Đoạn GPS muộn nhất dùng cho tốc độ lịch sử | 2014-04-30 23:59:59 (trước mốc chia: đạt) |
| Tham số cố định trước khi chạy, test không dùng để tinh chỉnh | Đúng |

## Khoảng ước lượng 80% (mở rộng ngoài đề, đã duyệt)

Hiệu chỉnh trên sai số tháng 04/2014 của một model chỉ học dữ liệu trước đó. Độ bao phủ đo một lần trên tập test: **80.35%** (mục tiêu 80%), độ rộng trung vị 9.8 phút. Chi tiết: `interval_validation.md`.

## Feature thời gian theo đồ thị (đã hứa ở Session 9)

Đồ thị chỉ xây từ train, chuyến huấn luyện dùng đồ thị của các tuần khác, quyết định trên 04/2014: MAE GBT 4.199 → 4.117, **giữ**. Trên test: 4.192 → 4.105, R² 0.228 → 0.240. Chi tiết: `graph_features.md`. Nhóm giữ GBT 6 feature để phục vụ (cải thiện chỉ khoảng 2%, phục vụ model mới cần thêm đồ thị chỉ từ train lúc dự đoán); đưa model có đồ thị vào phục vụ là hướng phát triển [P].

## Model đã lưu (mục 8)

`models/spark_{linear,gbt}_{train_all,train_no_gps_jump}` (Spark ML) và `models/features.json` (thứ tự feature, nhãn, mốc chia, tham số). Model phục vụ được **chọn trước khi chạy**: `spark_gbt_train_all`, huấn luyện chỉ trên giai đoạn train, nên phép thử F3 (một chuyến từ CSV đến dự đoán) sẽ là ngoài mẫu. `models/` không commit lên git. Có dùng bản sklearn cho giao diện hay không sẽ quyết ở Phase 5, và nếu dùng thì phải đánh giá lại trên cùng tập test và ghi rõ.

## Chưa làm (giữ đúng phạm vi đã thống nhất)

- Feature từ đồ thị tuyến (đề mục 7, cải tiến 5): **chưa làm**. Thiết kế đã có (đồ thị chỉ xây từ train), nhưng nhóm chọn giữ ML ở mức đề yêu cầu.
- Không so với random split, không tinh chỉnh siêu tham số.

Thời gian huấn luyện (giây): {'linear/train_all': 1.0, 'gbt/train_all': 21.0, 'linear/train_no_gps_jump': 0.5, 'gbt/train_no_gps_jump': 18.5}. Số tham chiếu lần trước (MAE GBT 4,02) chỉ để đối chiếu cho hợp lý, không dùng làm kết quả.
