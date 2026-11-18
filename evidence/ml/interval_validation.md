# Khoảng ước lượng 80% cho ETA (mở rộng ngoài đề, đã duyệt; số đo thật)

Nguồn: `interval_validation.json` (`src/ml/job_eta_interval.py`), bảng dùng khi phục vụ: `models/interval.json`.

**Mục đích:** MAE 4,21 phút là sai số trung bình trên mọi chuyến, không cho biết sai số của một chuyến cụ thể. Khoảng ước lượng cho người dùng biết "thường từ bao nhiêu đến bao nhiêu phút".

**Cách làm (quy tắc chốt trước khi chạy, không chỉnh theo kết quả test):**
1. Model hiệu chỉnh: GBT cùng tham số với model phục vụ, chỉ học chuyến **trước 01/04/2014**.
2. Tháng hiệu chỉnh: **04/2014** (133,451 chuyến), vẫn thuộc giai đoạn train. Sai số = thực tế − dự đoán.
3. Chia 10 nhóm theo độ dài dự đoán (mỗi nhóm cùng số chuyến). Mỗi nhóm lấy phân vị 10% và 90% của sai số.
4. Khoảng = dự đoán của **model phục vụ** + [phân vị 10%, phân vị 90%] của nhóm tương ứng, tối thiểu 0,25 phút.
5. Tập test 05–06/2014 chỉ dùng **một lần** để đo độ bao phủ.

**Không leakage:** bảng sai số tính hoàn toàn từ dữ liệu trước 05/2014. Model hiệu chỉnh học đến 03/2014, model phục vụ học đến 04/2014; áp bảng của model này cho model kia là một xấp xỉ, và độ bao phủ đo trên test cho thấy xấp xỉ này dùng được.

## Kết quả trên tập test (308,543 chuyến)

| | Giá trị |
|---|---|
| Mục tiêu | 80% |
| **Độ bao phủ đo được** | **80.35%** |
| Như trên, bỏ chuyến có bước nhảy GPS | 81.01% |
| Thực tế ngắn hơn khoảng / dài hơn khoảng | 9.81% / 9.84% |
| Độ rộng khoảng: trung bình / trung vị | 10.97 / 9.8 phút |

| Nhóm dự đoán (phút) | Phân vị 10% | Phân vị 90% | Chuyến hiệu chỉnh | Chuyến test | Bao phủ test | Độ rộng TB (phút) |
|---|---|---|---|---|---|---|
| < 7.2 | -3.46 | +3.87 | 13,345 | 28,101 | 81.2% | 7.3 |
| 7.2–8.2 | -4.13 | +3.94 | 13,345 | 30,154 | 80.5% | 8.1 |
| 8.2–9.0 | -4.34 | +4.68 | 13,345 | 27,890 | 80.3% | 9.0 |
| 9.0–10.0 | -4.44 | +4.63 | 13,343 | 30,637 | 79.4% | 9.1 |
| 10.0–11.0 | -4.42 | +4.79 | 13,345 | 31,750 | 79.3% | 9.2 |
| 11.0–12.3 | -4.85 | +4.95 | 13,348 | 31,193 | 78.0% | 9.8 |
| 12.3–13.6 | -5.91 | +5.75 | 13,345 | 30,758 | 83.5% | 11.7 |
| 13.6–15.4 | -5.77 | +5.69 | 13,343 | 32,160 | 80.4% | 11.5 |
| 15.4–17.7 | -6.39 | +6.57 | 13,340 | 32,721 | 80.3% | 13.0 |
| ≥ 17.7 | -9.92 | +9.89 | 13,352 | 33,179 | 80.7% | 19.8 |

**Đọc kết quả:**
- Độ bao phủ trên test (80.3%) sát mục tiêu 80%, và mọi nhóm đều trong khoảng 78–84%: khoảng hiệu chỉnh trên tháng 4 dùng được cho tháng 5–6.
- Khoảng **rộng dần theo độ dài chuyến**: khoảng 7 phút với chuyến ngắn, khoảng 20 phút với chuyến dự đoán trên 18 phút.
- Khoảng **gần như cân hai bên** dự đoán (số chuyến rơi ra mỗi phía đều khoảng 9,8%). Điều này khác với giả thuyết ban đầu của nhóm (khoảng sẽ lệch về phía dài); số đo cho thấy model đã kéo dự đoán lên để bù cho các chuyến rất dài.
- Khoảng rộng phản ánh đúng giới hạn của bài toán (R² khoảng 0,22 khi chỉ biết A, B và thời điểm). Đây không phải lỗi.

**Ví dụ F3:** chuyến `1399135956620000324`: thực tế 7.75 phút, dự đoán 8.16 phút, khoảng 4.03–12.1 phút (thực tế nằm trong khoảng). Ảnh: `screenshots/ui_sample_trip_interval.jpg`.

`estimate()` trả thêm trường `predicted_interval_min` bên cạnh 4 trường của đề. Giao diện hiển thị "dự kiến X phút, thường từ A đến B phút".
