# Feature thời gian theo đồ thị tuyến (đã hứa ở Session 9; số đo thật)

Nguồn: `graph_features.json` (`src/ml/job_graph_features.py`), hàm tính feature `src/ml/graph_features.py`, test `tests/test_graph_features.py`.

## Feature thêm vào

| Feature | Ý nghĩa |
|---|---|
| `graph_time_min` | thời gian tuyến nhanh nhất từ zone A tới zone B trên đồ thị zone, cạnh tính theo tốc độ zone × giờ × loại ngày |
| `graph_km` | quãng đường ngắn nhất trên đồ thị (cạnh = khoảng cách tâm-tâm): cho biết phải đi vòng bao nhiêu so với đường thẳng |
| `graph_found` | 1 nếu tìm được tuyến; 0 nếu điểm đầu hoặc cuối không thuộc đồ thị (khi đó hai cột trên = −1) |

Cả ba chỉ cần A, B và thời điểm xuất phát, nên **có trước khi xe chạy**.

## Chống leakage

- Đồ thị và tốc độ zone × giờ **chỉ xây từ đoạn GPS trước mốc chia**: đoạn muộn nhất dùng ở giai đoạn validation là 2014-03-31 23:59:59, ở giai đoạn test là 2014-04-30 23:59:59.
- Với chuyến dùng để **huấn luyện**: đồ thị xây từ **các tuần khác** (chia tuần thành 5 nhóm theo số tuần mod 5), để chuyến không thấy dữ liệu của chính nó qua đồ thị. Unit test kiểm tra phép loại trừ này.
- Quyết định giữ hay bỏ dựa trên **tháng 04/2014** (validation, thuộc giai đoạn train). Tập test chỉ chạy **sau khi** đã quyết định.
- Tham số GBT giống model chính, không tinh chỉnh.

## Bước 1: quyết định trên validation (04/2014)

Huấn luyện trên 07/2013–03/2014 (1,231,185 chuyến), đánh giá trên 04/2014 (133,451 chuyến). Tìm được tuyến cho 99.4% chuyến validation.

| Model | MAE (phút) | RMSE | R² | Signed error |
|---|---|---|---|---|
| GBT, 6 feature của đề | 4.199 | 8.398 | 0.227 | +0.039 |
| **GBT, 6 feature + 3 feature đồ thị** | 4.117 | 8.335 | 0.239 | +0.041 |
| Linear, 6 feature | 4.622 | 8.730 | 0.165 | +0.049 |
| Linear, 6 + 3 feature đồ thị | 4.315 | 8.543 | 0.201 | +0.033 |

**Quy tắc chốt trước:** giữ feature đồ thị nếu MAE của GBT trên 04/2014 giảm. MAE GBT 4.199 → 4.117 (giảm 0.082 phút, 1.9%). **Quyết định: GIỮ.**

## Bước 2: đo trên tập test một lần (05–06/2014)

Huấn luyện lại trên toàn bộ train (1,364,636 chuyến), đánh giá trên 308,543 chuyến test. Tìm được tuyến cho 99.4% chuyến test.

| Model | MAE (phút) | RMSE | R² | Signed error |
|---|---|---|---|---|
| GBT, 6 feature của đề | 4.192 | 8.270 | 0.228 | +0.052 |
| **GBT, 6 feature + 3 feature đồ thị** | 4.105 | 8.205 | 0.240 | +0.038 |
| Linear, 6 feature | 4.566 | 8.560 | 0.173 | +0.034 |
| Linear, 6 + 3 feature đồ thị | 4.285 | 8.381 | 0.207 | +0.067 |

Mức quan trọng của feature (GBT có đồ thị): straight_km 0.33, **graph_time_min 0.27**, hour 0.16, bearing_deg 0.13, graph_km 0.05, các feature còn lại dưới 0,03.

## Đọc kết quả

- Feature đồ thị **giúp thật nhưng không nhiều**: MAE GBT trên test giảm từ 4.192 xuống 4.105 phút (2.1%), R² tăng từ 0.228 lên 0.240. Mức giảm trên test gần bằng mức giảm trên validation, đúng với việc quyết định không dựa vào test.
- `graph_time_min` trở thành feature quan trọng thứ hai sau đường thẳng: model dùng được thông tin "phải đi vòng" mà đường thẳng không có.
- Linear Regression cải thiện nhiều hơn (MAE 4.566 → 4.285) vì nó không tự học được quan hệ phi tuyến như GBT.
- R² vẫn thấp (khoảng 0,24): phần lớn biến động thời gian chuyến vẫn không đoán được khi chỉ biết A, B và giờ.

**Lưu ý về độ dao động:** GBT 6 feature ở đây ra MAE 4.192, còn ở `E_model_validation.md` là 4.213, dù cùng dữ liệu, cùng tham số. Khác biệt khoảng 0.02 phút đến từ việc Spark chia dữ liệu khác nhau (phép join làm đổi thứ tự và phân vùng, ảnh hưởng cách GBT chia bin). Vì vậy phép so sánh công bằng là **hai model trong cùng job này**: mức cải thiện 0.087 phút lớn hơn khoảng dao động đó.

## Quyết định của nhóm về phục vụ

Model có feature đồ thị **đã huấn luyện, đã đo và đã lưu** (`models/spark_gbt_graph_train_all`, `models/features_graph.json`). Nhóm **giữ model 6 feature để phục vụ** (giao diện và REST API), vì:
- mức cải thiện chỉ khoảng 2% MAE;
- muốn phục vụ model mới phải tính feature lúc dự đoán bằng đồ thị chỉ xây từ dữ liệu trước 05/2014 (khác đồ thị cả năm đang dùng để vẽ tuyến trong MongoDB) và hiệu chỉnh lại khoảng ước lượng 80%, trong khi thời gian còn lại không đủ để làm và kiểm tra lại đầy đủ.

Đưa model có feature đồ thị vào phục vụ là **hướng phát triển [P]**.
