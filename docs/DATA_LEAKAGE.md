# Chống data leakage: quy tắc và cách chứng minh

> **Trạng thái: ĐÃ KIỂM (Phase 4).** Các kiểm tra ở mục 4 đã chạy tự động trong `src/ml/job_train_eta.py` và unit test `tests/test_ml.py`; kết quả ở `evidence/ml/leakage_checks_full.json` và `evidence/ml/E_model_validation.md`. Báo cáo cả có lọc và không lọc nhãn nhiễu: đã duyệt và đã làm. Feature từ đồ thị tuyến: chưa làm.

Dùng cho Card F1/F2 và để trả lời thầy. Nguyên tắc duy nhất: **khi dự đoán một chuyến, model chỉ được biết những gì hệ thống biết tại thời điểm khách đặt xe.**

## 1. Bảng feature: có trước khi xuất phát hay không

| Feature | Lấy từ | Biết trước khi xuất phát? | Dùng |
|---|---|---|---|
| `straight_km` | haversine(A, B) | Có: A và B do khách nhập | Có |
| `bearing_deg` | hướng A → B | Có | Có |
| `hour` | giờ địa phương lúc xuất phát | Có | Có |
| `day_of_week`, `is_weekend` | ngày địa phương | Có | Có |
| `is_holiday` | `holidays.csv` (lịch công bố trước) | Có | Có |
| số điểm GPS, độ dài `POLYLINE` | chính quỹ đạo | **Không**: nhãn = (số điểm − 1) × 15 s | **Cấm** |
| quãng đường thực tế đã đi, tốc độ trung bình của chuyến | chính quỹ đạo | Không | **Cấm** |
| thời điểm kết thúc, các điểm giữa chuyến | chính quỹ đạo | Không | **Cấm** |
| `zone_hour_speed` tính trên toàn bộ dữ liệu | cả train và test | Không (chứa tương lai) | **Cấm** làm feature; chỉ được dùng bản tính từ train |
| `TAXI_ID` | cột gốc | Có, nhưng để model ghi nhớ từng xe | Không dùng (không nằm trong thiết kế) |

Điểm B là **điểm cuối của polyline trong dữ liệu**, nhưng trong bài toán này B do khách nhập lúc đặt xe, nên được phép. Báo cáo phải nói rõ: đây là *known-destination ETA*, không dự đoán điểm đến.

## 2. Năm quy tắc

1. **Chia theo thời gian, không chia ngẫu nhiên.** Train: chuyến có thời điểm xuất phát (giờ địa phương) trước 01/05/2014. Test: 01/05 đến 30/06/2014. Chia theo thời điểm xuất phát, không theo thời điểm kết thúc.
2. **Test chỉ dùng một lần để báo cáo.** Chọn siêu tham số và so sánh model trên một đoạn validation nằm cuối giai đoạn train (ví dụ 04/2014), không nhìn test.
3. **Mọi thứ "học" từ dữ liệu chỉ fit trên train:** bộ mã hóa, bộ chuẩn hóa, ngưỡng, và đặc biệt là baseline tốc độ lịch sử và đồ thị tuyến.
4. **Hai phiên bản bảng tổng hợp:**
   - `*_train`: tính chỉ từ chuyến trước 01/05/2014, dùng cho baseline, feature đồ thị và đánh giá.
   - Bản đủ dữ liệu: chỉ dùng cho hiển thị trên Streamlit, không được đưa vào số liệu đánh giá.
5. **Model phục vụ được huấn luyện trên giai đoạn train** (như tài liệu yêu cầu mục 7.4), để phép thử chạy một chuyến thật (F3) là ngoài mẫu. Chuyến demo F3 lấy từ tập test.

## 3. Chỗ dễ lọt nhất

- **Lọc nhãn nhiễu (khoảng 10% do mất GPS):** quy tắc lọc (bước nhảy > 150 km/h) chọn dựa trên train. Báo cáo kết quả **cả có lọc và không lọc** trên test, để không ai nghi ngờ việc lọc test làm đẹp số.
- **Dedup, trễ, watermark của streaming** không liên quan đến ML nên không gây leakage, nhưng nhớ không dùng dữ liệu live để huấn luyện.
- **Chuyến trùng hoặc gần trùng giữa train và test:** chia theo thời gian đã tránh phần lớn, nhưng vẫn kiểm tra `TRIP_ID` không xuất hiện ở cả hai tập.
- **Cột `DAY_TYPE`:** toàn `A`, không mang thông tin, không dùng.

## 4. Kiểm tra tự động (đưa vào unit test, ghi vào `evidence/`)

| Kiểm tra | Điều kiện đạt |
|---|---|
| Tách thời gian | `max(thời điểm xuất phát train) < min(thời điểm xuất phát test)` |
| Không trùng chuyến | giao của `TRIP_ID` train và test là rỗng |
| Danh sách feature | tập feature của model là tập con của danh sách cho phép ở mục 1 |
| Khớp lúc phục vụ | `features.json` trùng thứ tự với feature khi huấn luyện |
| Bảng tổng hợp | các bảng `*_train` chỉ chứa chuyến trước 01/05/2014 |
| Nhãn | không có cột feature nào tính từ `POLYLINE` ngoài điểm đầu và điểm cuối |

## 5. Trả lời thầy trong một đoạn

*Các kiểm tra ở mục 4 đã chạy (`evidence/ml/leakage_checks_full.json`). Đoạn dưới đã được cập nhật cho khớp với những gì thực sự làm: không chọn siêu tham số (tham số cố định), không dùng feature đồ thị.*

> Model chỉ nhận điểm đi, điểm đến, giờ và ngày, tất cả khách cung cấp lúc đặt xe. Nhãn là độ dài chuỗi GPS nên mọi đại lượng tính từ chuỗi đó đều bị cấm. Em chia theo thời gian (train trước 05/2014, test 05–06/2014), baseline tốc độ lịch sử chỉ tính từ đoạn GPS trước mốc chia, tham số model cố định trước khi chạy nên tập test chỉ dùng một lần để báo cáo. Em báo cáo kết quả cả khi lọc và không lọc nhãn nhiễu, và có unit test kiểm tra tách thời gian, không trùng chuyến và danh sách feature.
