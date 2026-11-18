# Phase 3: MapReduce trên YARN + HDFS và đối chiếu với Spark (số đo thật)

Chạy bằng `scripts/run_mapreduce.sh` (sample 20.000 dòng) rồi `--full`. Nguồn số liệu: `reconcile_full.json`, `reconcile_sample.json`, log job `job_full.log`, `yarn_apps_full.json`. Unit test: 15 test qua (`evidence/validation/phase3_tests.txt`).

## Job

- Hadoop Streaming 3.5.0 trên YARN, đọc **raw CSV từ HDFS** (`/porto/raw/full/train.csv`, 1,942,911,619 byte đã đọc).
- Mapper, combiner, reducer viết bằng Python. Combiner và reducer là **cùng một script**, chỉ cộng dồn `total_km`, `total_s`, `n_segments`.
- Khóa là `zone_id,hour` (giờ địa phương Lisbon).

| Chỉ số | Giá trị |
|---|---|
| Trạng thái YARN | FINISHED / SUCCEEDED |
| Thời gian chạy theo YARN (`elapsedTime`) | 129.9 s |
| Thời gian từ lúc submit đến khi xong (đo bằng script) | 138 s |
| Map task / reduce task | 15 / 2 |
| Map input records (gồm 1 dòng header) | 1,710,671 |
| Map output records (số đoạn phát ra) | 79,502,909 |
| Combine input → output records | 80,459,674 → 1,353,024 |
| Reduce input records | 396,259 |
| Reduce shuffle bytes | 13,875,926 |
| Reduce output records (số key) | 40,548 |

Combiner giảm dữ liệu trước shuffle từ khoảng 80 triệu bản ghi xuống 1,353,024. *Combine input* lớn hơn *map output* vì combiner chạy nhiều lần (khi spill và khi merge), và reduce input nhỏ hơn combine output vì combiner chạy thêm khi merge ở phía reduce. Cả hai đều hợp lệ vì combiner chỉ cộng dồn.

Hadoop chạy trên image amd64 qua giả lập trên Mac ARM, nên thời gian chạy **không đại diện** cho máy amd64 thật và không so với số tham chiếu lần trước (68 s).

## Counter của mapper so với bronze (Spark)

| Lý do | MapReduce | Spark bronze | Khớp |
|---|---|---|---|
| missing_data | 10 | 10 | có |
| bad_json | 0 | 0 | có |
| too_few_points | 36,508 | 36,508 | có |
| too_long | 847 | 847 | có |
| valid | 1,673,305 | 1,673,305 | có |

Mapper còn đếm: cặp điểm ngoài bbox 1,073,615, cặp quá 150 km/h trong bbox 198,844, dòng CSV sai số cột 0. Hai số đầu khớp với số Spark đếm ở Phase 2.

## Đối chiếu từng key (zone_id, hour)

Spark tính lại từ **cùng file raw** bằng chính các hàm bronze và silver của pipeline. Ngưỡng sai số km theo đề: `n_segments × 1e-6`.

**1. So với Spark SQL cùng logic với MapReduce** (MapReduce đọc từng dòng nên đếm cả bản ghi lặp):

| | Giá trị |
|---|---|
| Số key MapReduce / Spark | 40,548 / 40,548 |
| Key chỉ có ở một bên | 0 / 0 |
| Key lệch `n_segments` hoặc `total_s` | 0 |
| Key lệch km vượt ngưỡng | 0 |
| Sai số km lớn nhất | 9.09e-12 km |
| Sai số lớn nhất / ngưỡng cho phép | 3.29e-10 |
| Tổng đoạn | 79,502,909 = 79,502,909 |

**Kết quả: 40,548 / 40,548 key khớp, 0 key lệch.**

**2. So với bảng silver của pipeline** (silver bỏ chuyến ghi lặp):
lệch đúng **1 key** (zone 1576, giờ 0), MapReduce nhiều hơn 10 đoạn. Con số này giải thích được trọn vẹn: chuyến `1397172149620000454` bị ghi 3 lần, silver giữ 1 bản, MapReduce đếm cả 3. Có 2 bản lặp, mỗi bản 5 đoạn rơi vào cùng key đó, tổng 10 đoạn. Tổng đoạn chênh 10.

**Bản sample** (20.000 dòng đầu): 16,059 key khớp, 0 key lệch.

## Vai trò

MapReduce chỉ dùng để **đối chiếu độc lập** với Spark: kết quả không ghi vào MongoDB và không phục vụ API. Bảng `zone_hour_speed` của pipeline có thêm chiều `is_weekend` nên có nhiều dòng hơn; phép đối chiếu dùng một câu Spark SQL gom theo đúng khóa (zone, giờ) của MapReduce.

## Các lỗi đã gặp và cách xử lý (như đề cảnh báo)

- `-files` đặt file dưới dạng symlink ở thư mục làm việc của task: mapper gọi `sys.path.insert(0, os.getcwd())` trước khi import `mr_common`.
- Không dùng `yarn node -list` để chờ YARN (lệnh này treo): script poll REST `http://localhost:8088/ws/v1/cluster/metrics` đến khi `activeNodes ≥ 1`.
- Hadoop và Spark không chạy cùng lúc: script dừng Spark trước và dừng Hadoop sau khi lấy kết quả.

**Chưa có trong phase này:** ảnh chụp YARN UI cho báo cáo (cần chạy lại job khi chụp, để ở phần tổng kết).
