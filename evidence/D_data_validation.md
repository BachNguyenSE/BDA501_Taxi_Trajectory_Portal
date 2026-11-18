# D. Kiểm tra dữ liệu (số đo thật)

Gom từ `bronze/`, `eda/`, `spark/D_counts_full.json`, `spark/noise_profile_full.json`, `mapreduce/reconcile_full.json`.

## Số dòng qua từng tầng

| Tầng | Số dòng | Ghi chú |
|---|---|---|
| raw `train.csv` | 1,710,670 | 1.942.848.724 byte |
| bronze (giữ mọi dòng) | 1,710,670 | hợp lệ 1,673,305; too_few_points 36,508; too_long 847; missing_data 10; bad_json 0 |
| silver/trips | 1,673,303 | bỏ 2 dòng ghi lặp cùng `trip_uid` (một chuyến ghi 3 lần) |
| silver/segments | 79,502,899 | từ 80,775,358 cặp điểm: bỏ 1,073,615 ngoài bbox, 198,844 quá 150 km/h |
| gold/zones | 2,142 | |
| gold/zone_hour_speed | 72,142 | |
| gold/zone_graph_edges | 9,444 | |
| gold/trip_features | 1,673,303 | train 1,364,636, test 308,543, ngoài khoảng 124 |

**Đối soát số đoạn:** đếm độc lập bằng mảng (không qua window) ra 79,502,899, bảng silver có 79,502,899: **khớp**. `trip_uid` duy nhất: True.

## Null và khoảng giá trị

- Null ở 7 feature và nhãn của `trip_features`: 0. `origin_zone` null 1,230, `dest_zone` null 9,481 (điểm đầu hoặc cuối ngoài bbox).
- Null ở `zone_hour_speed` (khóa, total_km, total_s): 0.
- Thời gian (UTC): 2013-07-01 00:00:53 đến 2014-06-30 23:59:56. Thời lượng chuyến hợp lệ: min 0.25, trung vị 10.25, max 180.0 phút.
- `bearing_deg` trong [0.00, 360.00]. `avg_speed_kmh` của zone × giờ trong [0.0, 148.9].
- Chuyến có ít nhất một bước nhảy GPS > 150 km/h: 10.42%. Điểm ngoài bbox: 1.30%.

## Đối soát ETL (Spark) ↔ MapReduce

| | MapReduce | Spark | Khớp |
|---|---|---|---|
| missing_data | 10 | 10 | có |
| bad_json | 0 | 0 | có |
| too_few_points | 36,508 | 36,508 | có |
| too_long | 847 | 847 | có |
| valid | 1,673,305 | 1,673,305 | có |
| Số key (zone, giờ) | 40,548 | 40,548 | 0 key lệch |
| Tổng đoạn | 79,502,909 | 79,502,909 | sai số km lớn nhất 9.1e-12 |

`too_long` = **847** ở cả hai bên: MapReduce áp dụng đúng bộ lọc chuyến > 180 phút như Spark, nên không còn chênh 847 chuyến như lần làm trước. So với bảng silver (đã bỏ chuyến ghi lặp), MapReduce nhiều hơn đúng 10 đoạn ở 1 key: đó là 2 bản lặp × 5 đoạn của chuyến ghi 3 lần.
