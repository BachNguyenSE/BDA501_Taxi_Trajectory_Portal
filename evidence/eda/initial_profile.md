# Đo sơ bộ `train.csv` (chưa phải EDA chính thức)

Phương pháp: script Python chạy tạm trong phiên làm việc, đọc bằng module `csv`. **Script chưa được lưu vào repo nên chưa tái lập được từ repo.** Phase 1 sẽ đo lại bằng `run_eda.sh` và thay file này.

## Toàn bộ file (đọc hết 1.710.670 dòng)

| Chỉ số | Giá trị |
|---|---|
| Kích thước | 1.942.848.724 byte |
| Số chuyến (dòng) | 1.710.670 |
| `MISSING_DATA = True` | 10 |
| `MISSING_DATA = False` | 1.710.660 |
| `CALL_TYPE` A / B / C | 364.770 / 817.881 / 528.019 |
| `DAY_TYPE` | 100% là `A` (không có B, C) |
| Khoảng thời gian (UTC) | 2013-07-01 00:00:53 đến 2014-06-30 23:59:56 |

## Mẫu 150.000 dòng đầu của file

Mẫu theo thứ tự file, thiên về những tháng đầu, **không đại diện cho cả năm**.

| Chỉ số | Giá trị |
|---|---|
| Chuyến dùng được (JSON hợp lệ, từ 2 điểm) | 147.588 |
| Số điểm GPS | 7.114.939 |
| Điểm nằm trong bbox Porto | 98,2% |
| Hai điểm liên tiếp cùng zone | 72,7% |
| Hai điểm liên tiếp ở zone kề nhau | 27,0% |
| Hai điểm liên tiếp nhảy qua ít nhất 1 zone | 0,4% |
| Zone có xe đi qua (lưới 70 × 50 = 3.500) | 1.863 |
| Zone có từ 100 điểm trở lên | 1.029 |

Lưới: 0,005°, bbox lon −8,80 đến −8,45, lat 41,05 đến 41,30. Zone của điểm tính theo từng điểm, không phải trung điểm đoạn như đề quy định cho silver, nên số này chỉ để ước lượng khả thi.
