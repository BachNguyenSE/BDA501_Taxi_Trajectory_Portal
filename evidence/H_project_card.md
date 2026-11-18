# H. Project card: Porto Taxi ETA

**Môn:** BDA501 · **Challenge card:** F (Porto Taxi GPS Trip Duration & ETA) · **Nhóm:** 4 (phân công: `team.md`)

## Bài toán

Người dùng nhập điểm A, điểm B và thời điểm xuất phát. Hệ thống trả về:
1. thời gian dự kiến (phút),
2. các zone có khả năng tắc theo tốc độ lịch sử zone × giờ,
3. tuyến gợi ý là chuỗi zone từ A đến B,
4. các zone **lúc này** chậm hơn bình thường, lấy từ luồng streaming (phát lại dữ liệu lịch sử, không phải real-time).

Phạm vi là **known-destination ETA**: B do khách nhập lúc đặt xe nên được dùng; model chỉ dùng thông tin có trước khi xuất phát.

## Dữ liệu

- Porto taxi trajectories (ECML/PKDD 2015, CC BY 4.0): 1,673,303 chuyến hợp lệ trên 1.710.670 dòng, khoảng 82 triệu điểm GPS, 07/2013–06/2014, 15 giây một điểm.
- `holidays.csv`: 10 ngày lễ do nhóm tự tổng hợp (9 lễ quốc gia + São João của Porto), vì cột `DAY_TYPE` trong dữ liệu toàn là `A`.
- NYC TLC Yellow Taxi bị loại: chỉ có mã zone, không có quỹ đạo GPS.

## Công nghệ và vai trò

| Công nghệ | Vai trò trong bài | Vì sao không dùng cách đơn giản hơn |
|---|---|---|
| Parquet data lake (raw, bronze, silver, gold) | Lưu từng tầng, chạy lại từ tầng trước | Định dạng cột, nén tốt; tách tầng giữ được lý do loại từng dòng |
| Spark (DataFrame + SQL) | Tách 79,502,899 đoạn GPS, tính bảng gold, feature | ~82 triệu điểm, pandas một máy không xử lý nổi với 8 GB RAM |
| Hadoop MapReduce (YARN + HDFS) | Đối chiếu độc lập kết quả Spark theo từng key | Hai công cụ khác nhau ra cùng kết quả thì mới tin được pipeline |
| Spark ML | Linear Regression, GBT trên 308,543 chuyến test | Cùng hệ sinh thái với dữ liệu đã ở Spark |
| MongoDB | Tra cứu theo khóa cho giao diện; nơi batch và streaming cùng ghi vào collection riêng | Parquet không có index tra một dòng; đo được IXSCAN p50 0.121 ms |
| Kafka | Hàng chờ giữa producer và Spark | **Không phải vì throughput** (khoảng 200 tin/giây khi phát ×60): để tách producer khỏi consumer, đọc lại theo offset, nhiều consumer độc lập |
| Spark Structured Streaming | Watermark, bỏ trùng, cửa sổ trượt theo zone | Xử lý tin trễ và trùng theo thời gian sự kiện |
| Streamlit | Giao diện | Viết nhanh bằng Python, đúng sơ đồ đã duyệt |

## Kết quả chính (số đo thật)

| Hạng mục | Kết quả | Bằng chứng |
|---|---|---|
| ETA, temporal hold-out | GBT MAE **4.21** phút (naive 5.55, Linear 4.57), R² 0.22 | `ml/E_model_validation.md` |
| Khoảng ước lượng 80% (mở rộng) | Bao phủ **80.3%** trên test, độ rộng trung vị 9.8 phút | `ml/interval_validation.md` |
| Leakage | Kiểm tra tự động đạt: tách thời gian, 0 chuyến ở cả hai tập, baseline chỉ từ train | `ml/leakage_checks_full.json` |
| Spark ↔ MapReduce | 40,548 / 40,548 key khớp, 0 lệch | `mapreduce/phase3_summary.md` |
| Broadcast vs sort-merge join | Shuffle 0 MB so với 51.3 MB; thời gian trung vị 0.239 s so với 0.286 s | `spark/B_benchmark.md` |
| MongoDB | IXSCAN, 1 tài liệu đọc; p50 0.121 ms | `mongo/G_nosql.md` |
| Streaming | 42,395 message; JSON hỏng 81/81; bản trùng 413/413; bản trễ 208/223 (15 bản còn lại nằm trong watermark ở cuối khung, đã đo) | `streaming/summary.md` |

## Phát hiện về dữ liệu

- `TRIP_ID` **không duy nhất**: cùng xe, cùng giây xuất phát. Đã thêm khóa `trip_uid`. Một chuyến bị ghi 3 lần, silver giữ một bản.
- Zone gần sân bay có tốc độ 1,65 km/h giờ cao điểm vì 91,9% đoạn đứng yên: đó là khu xe chờ, không phải tắc đường.
- Các zone trên trục cao tốc từ sân bay vào thành phố có tốc độ trung vị khoảng 94 km/h, khác hẳn trung vị toàn thành phố 21 km/h.
- 10,4% chuyến có bước nhảy GPS > 150 km/h; bỏ chúng khỏi tập test làm MAE giảm từ 4,21 xuống 4,03 phút. Số báo cáo chính là bản không lọc.

## Giới hạn

- R² chỉ khoảng 0,22: chỉ với A, B và thời điểm, phần lớn biến động thời gian chuyến không giải thích được.
- Baseline tốc độ lịch sử tệ hơn naive; giả thuyết (chưa đo) là đường thẳng ngắn hơn đường thật và tốc độ zone gồm cả lúc đứng chờ.
- Tuyến ở mức zone ~420 × 555 m, không phải đường phố. Thời gian theo đồ thị bằng khoảng 0,71 lần thực tế.
- Hadoop chạy qua giả lập amd64 trên Mac ARM nên thời gian chạy không đại diện cho máy thật.
- Streaming là bản phát lại ×60 dữ liệu năm 2014, **không phải real-time**.

## Chỉ thiết kế [P]

Nguồn GPS live thật; lịch ETL hằng ngày và huấn luyện lại hằng tháng; HDFS hoặc S3 cho cụm; REST `POST /estimate`; feature thời gian theo đồ thị tuyến cho model; map-matching với OpenStreetMap.

## Tái lập

Xem `README.md` ở thư mục gốc. Seed 42, `PYTHONHASHSEED=42`, Spark session UTC, phiên bản cố định (Python 3.12, JDK 17, PySpark 4.0.1, Hadoop 3.5.0, MongoDB 8.0, Kafka 3.9.1), cấu hình tập trung ở `conf/pipeline.yaml`.
