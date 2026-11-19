# Thay đổi từ Session 9 đến bản final

Tài liệu này đối chiếu **báo cáo Session 9** (`BDA501_Session9_Readiness_Report_PortoTaxi_G04.docx`) với **bản final**, để thầy và nhóm hiểu vì sao một số con số khác nhau.

Quy ước:
- Cột **Session 9** chép từ báo cáo Session 9. Nhóm **không đo lại** các số này ở bản final.
- Cột **Final** là số đo thật của bản hiện tại, kèm file bằng chứng.
- Phần lớn thay đổi là **sửa lỗi hoặc làm đúng yêu cầu hơn**. Mỗi thay đổi đều ghi lý do.

Tóm tắt một câu khi trình bày: *"Bản final xây lại pipeline theo đúng sơ đồ đã duyệt, sửa các điểm Session 9 tự nhận là còn yếu (model phục vụ học trên split ngẫu nhiên, MapReduce lọc khác Spark, chia train/test theo UTC), nên một số con số thay đổi; kết luận chính không đổi."*

---

## 1. Những gì KHÔNG đổi

- Dữ liệu: Porto taxi trajectories (ECML/PKDD 2015), `train.csv` 1,94 GB, 1.710.670 chuyến. NYC TLC vẫn bị loại vì không có quỹ đạo GPS.
- Phạm vi bài toán: **known-destination ETA**. B là input người dùng lúc đặt xe.
- MapReduce chỉ dùng để **đối chiếu độc lập** với Spark, không nằm trên đường phục vụ.
- Lớp live **chỉ đánh dấu**, không bao giờ đổi ETA; không có online learning.
- Streaming là **phát lại** (replay) ngày 14/05/2014 16:00–19:00 giờ Lisbon, tốc độ ×60, **không phải real-time**.
- Kết luận chính: GBT tốt hơn naive khoảng một phần tư về MAE; R² khoảng 0,2 vì chỉ biết A, B và thời điểm; khoảng 10% nhãn bị đếm thiếu do GPS hụt điểm.

---

## 2. Dữ liệu và làm sạch

| Hạng mục | Session 9 | Final | Lý do |
|---|---|---|---|
| Tầng data lake | `data/raw` + `data/curated` | `raw` → `bronze` → `silver` → `gold` | Đúng sơ đồ đã duyệt. Bronze **giữ mọi dòng** kèm `reject_reason` (`missing_data`, `bad_json`, `too_few_points`, `too_long`) để đối soát được từng lý do loại. Bằng chứng: `evidence/bronze/phase1_summary.md` |
| Chuyến hợp lệ sau làm sạch | 1.673.305 | 1.673.305 (bronze) → **1.673.303** (silver) | Silver bỏ 2 dòng **ghi lặp** của cùng một chuyến (xem dòng dưới) |
| `TRIP_ID` trùng | "8 TRIP_ID trùng trong trip_features, giữ lại, gap đã biết" | **Đã xử lý.** `TRIP_ID` không duy nhất (cùng xe, cùng giây xuất phát) nên thêm khóa `trip_uid` = `TRIP_ID` + md5 polyline; một chuyến bị ghi 3 lần chỉ giữ 1 bản | Khi window `lag` chia nhóm theo `TRIP_ID`, điểm của hai chuyến khác nhau bị trộn thành đoạn giả. Lỗi lộ ra nhờ đối soát số đoạn (`evidence/spark/phase2_summary.md`) |
| Số dòng `trip_features` | 1.647.347 | **1.673.303** | Báo cáo Session 9 không ghi rõ bộ lọc thêm từ 1.673.305 xuống 1.647.347. Bản final giữ mọi chuyến hợp lệ; chuyến có điểm đầu hoặc cuối ngoài bbox (10.095 chuyến) vẫn giữ, `origin_zone` / `dest_zone` để null |
| Kiểm tra số đoạn silver | Không có | Đếm độc lập bằng mảng, **khớp tuyệt đối** 79.502.899 = 79.502.899 | Bằng chứng cho quy tắc "không lọc bbox trước `lag`" và cho việc sửa `TRIP_ID` trùng |
| Ngày lễ | Không có feature `is_holiday` | `holidays.csv` 10 ngày (9 lễ quốc gia + São João), feature `is_holiday` | Đề yêu cầu (mục [4]). Cột `DAY_TYPE` của dataset toàn là `A` nên phải tự tổng hợp; 4 ngày lễ bị Bồ Đào Nha bỏ trong 2013–2015 đã loại |

---

## 3. MapReduce

| Hạng mục | Session 9 | Final | Lý do |
|---|---|---|---|
| Bộ lọc của mapper | **Không** lọc chuyến > 180 phút, nên lệch Spark đúng 847 chuyến | Lọc **giống hệt** bronze (`missing_data` > `bad_json` > `too_few_points` > `too_long`) | Đề yêu cầu so từng key với Spark SQL "cùng logic". Counter `too_long` = 847 ở cả hai bên |
| Chuyến mapper dùng | 1.674.152 | 1.673.305 | Do lọc thêm 847 chuyến > 180 phút |
| Tổng đoạn | 80.025.093 | 79.502.909 | Như trên |
| Số key (zone, giờ) | 40.618 | **40.548** | Như trên |
| Khớp với Spark | 40.618 / 40.618 | **40.548 / 40.548**, sai số km lớn nhất 9×10⁻¹² | `evidence/mapreduce/phase3_summary.md` |
| Thời gian job | 68 s (VM amd64) | 129,9 s | Bản final chạy image Hadoop amd64 qua **giả lập trên Mac ARM**; không so trực tiếp với 68 s |
| Phần lệch còn lại | — | So với bảng silver lệch đúng 10 đoạn ở 1 key | Chính là 2 bản ghi lặp silver đã bỏ (MapReduce đọc từng dòng nên đếm cả) |

---

## 4. Spark SQL và thí nghiệm hiệu năng

| Hạng mục | Session 9 | Final | Lý do |
|---|---|---|---|
| Query dùng cho thí nghiệm | Q3 có `GROUP BY`, AQE bật, 64 shuffle partition | **Chỉ phép join** của Q3, ghi ra sink `noop`, 48 shuffle partition | Tách riêng chi phí của phép join, không lẫn chi phí `GROUP BY` |
| Shuffle: broadcast / sort-merge | 0,48 MB / 36,55 MB | **0 MB / 51,3 MB** | Bản Session 9 có thêm Exchange của `GROUP BY` nên broadcast vẫn có 0,48 MB; bản final đo riêng phép join nên broadcast không shuffle |
| Cách đo | Median runtime | 1 lần khởi động (không tính) + **7 lần đo** mỗi cách, xen kẽ; shuffle lấy từ Spark REST; loại join kiểm tra trên **plan cuối** sau khi chạy | Lần đầu đo 3 lần thì thời gian nằm trong nhiễu (lần chạy đầu chậm do khởi động) |
| Thời gian trung vị | 0,29 s / 0,33 s | 0,239 s / 0,286 s | Kết luận giống nhau: shuffle khác biệt rõ, thời gian chỉ chênh vừa phải khi chạy một máy |

Bằng chứng: `evidence/spark/B_benchmark.md`, `evidence/spark/C_query_plan_explained.md`.

---

## 5. Machine learning

| Hạng mục | Session 9 | Final | Lý do |
|---|---|---|---|
| Model **phục vụ** | sklearn GBT huấn luyện trên **split ngẫu nhiên 80/20**; báo cáo tự ghi F3 "có thể đã thấy" chuyến test | Spark GBT huấn luyện **chỉ trên giai đoạn train** (trước 05/2014), chọn trước khi chạy | Loại bỏ leakage ở F3: chuyến demo giờ chắc chắn là ngoài mẫu |
| Model đánh giá theo thời gian | sklearn `HistGradientBoostingRegressor` | Spark ML `GBTRegressor` (maxIter 50, maxDepth 6, seed 42) | Đề yêu cầu Spark ML; một model duy nhất cho cả đánh giá và phục vụ |
| Feature | 5 (khoảng cách thẳng, hướng, giờ, thứ, cuối tuần) | **6**, thêm `is_holiday` | Đúng danh sách feature của đề |
| Mốc chia train / test | 01/05/2014 **UTC** | 01/05/2014 **giờ Lisbon** | Đề quy định giờ và ngày tính theo giờ địa phương; chia theo cùng múi giờ cho nhất quán. 124 chuyến rơi sang 01/07/2014 theo giờ Lisbon bị loại khỏi test (đề: test 05–06/2014) |
| Số chuyến train / test | 1.343.537 / 303.810 | 1.364.636 / 308.543 | Do mốc giờ Lisbon và số dòng `trip_features` khác (mục 2) |
| Báo cáo split ngẫu nhiên | Có (bảng chính dùng split ngẫu nhiên) | **Không**, chỉ temporal hold-out | Đề yêu cầu đánh giá theo thời gian; tránh hiểu nhầm số split ngẫu nhiên là kết quả chính |
| Naive (MAE) | 5,40 | 5,545 | Khác tập test |
| Linear (MAE) | 4,35 | 4,566 | Khác thư viện, tập test, feature |
| GBT (MAE / R²) | 4,02 / ~0,24 | **4,213 / 0,221** | Khác thư viện, tham số, feature, mốc chia; **không so trực tiếp**. Cả hai đều giảm khoảng một phần tư so với naive |
| Baseline tốc độ lịch sử | Trung vị tốc độ **toàn thành phố** theo giờ × cuối tuần: MAE 5,46 | Tốc độ trung bình (tổng km / tổng giờ) của **zone xuất phát** × giờ × cuối tuần, chỉ từ đoạn GPS trước mốc chia: MAE 6,34 | Bản final dùng đúng tinh thần "tốc độ lịch sử theo vùng × giờ". Kết quả tệ hơn naive, nhóm giữ nguyên không chỉnh; giả thuyết (chưa đo): đường thẳng ngắn hơn đường thật và tốc độ zone gồm cả lúc xe đứng chờ |
| Nhãn nhiễu GPS | Ghi nhận là giới hạn (top correction #1) | **Báo cáo cả có lọc và không lọc** chuyến có bước nhảy > 150 km/h. Bỏ chúng khỏi test: MAE 4,21 → 4,03 | Đã làm điều hứa ở Session 9. Số báo cáo chính vẫn là bản không lọc |
| Kiểm tra leakage | Bảng F1 viết tay | Script **tự kiểm tra và dừng nếu vi phạm**: tách thời gian, 0 chuyến ở cả hai tập, baseline chỉ từ train, feature thuộc danh sách cho phép | `evidence/ml/leakage_checks_full.json` |
| Khoảng ước lượng | Không có | **Mới:** khoảng 80% hiệu chỉnh trên 04/2014, bao phủ 80,35% trên test | `evidence/ml/interval_validation.md` |

Bằng chứng: `evidence/ml/E_model_validation.md`.

---

## 6. NoSQL, tìm tuyến và giao diện

| Hạng mục | Session 9 | Final | Lý do |
|---|---|---|---|
| Giao diện | **FastAPI + Leaflet** (localhost:8000), `POST /estimate` | **Streamlit** (localhost:8501) **và FastAPI** `POST /estimate` (localhost:8000), cả hai dùng chung hàm `estimate()` | Sơ đồ đã duyệt ghi Streamlit; REST giữ lại vì đề có `/estimate` và Session 9 đã trình bày. API kiểm tra thứ tự feature khi khởi động (đề mục 8) |
| Độ trễ `/estimate` | p50 4,19 ms (sklearn) | p50 **6.44 ms**, p95 11.88 ms (Spark GBT `predict` trên driver) | `evidence/serving/api.md`. Không so trực tiếp: khác model, khác máy |
| `zones` / `zone_graph_edges` | 2.152 / 10.308 | 2.142 / 9.444 | Bản final chỉ giữ cạnh giữa hai zone **liền kề** (8 hướng) mà đoạn GPS thực sự đi qua; zone lấy từ trung điểm các đoạn giữ lại |
| Point read MongoDB | p50 0,15 ms, IXSCAN | p50 **0,121 ms**, IXSCAN, 1 tài liệu đọc; quét toàn bộ để so sánh: 72.142 tài liệu, 14,9 ms | `evidence/mongo/G_nosql.md` |
| Thuật toán tìm đường | networkx Dijkstra | scipy Dijkstra (`src/serving/routing.py`) | Không cần thêm thư viện, chạy nhanh |
| Cạnh dùng khi tìm đường | Mọi cạnh | Bỏ cạnh **dưới 5 lượt xe** | Không lọc thì 53% tuyến mẫu đi qua cạnh có số đo vô lý (một cạnh 1 lượt xe tính ra 6.656 s); lọc xong vẫn tìm được tuyến cho 3.000/3.000 chuyến mẫu. `evidence/spark/routing_check.md` |
| Thời gian cạnh | Từ tốc độ đo trên cạnh | Từ **tốc độ zone × giờ** (gồm cả lúc dừng) | Tốc độ trên cạnh chỉ gồm xe đang chạy (49,8 km/h so với 26,6 km/h mọi đoạn); tỉ lệ thời gian tuyến / thực tế tăng từ 0,34 lên 0,71 |
| Mức tắc trên giao diện | Theo tốc độ tuyệt đối | Tốc độ zone giờ đó **so với cả ngày của chính zone** (< 0,7 tắc, < 0,9 đông) | Zone 2685 gần sân bay (1,65 km/h, 91,9% đoạn đứng yên) là khu xe chờ, không bị tô đỏ cả ngày |
| Cách nhập A, B | Chọn trên bản đồ | 3 cách: 15 địa điểm có sẵn (tọa độ đã kiểm bằng dữ liệu), 20 chuyến mẫu từ tập test (so dự đoán với thực tế), chọn trên bản đồ | Người dùng không cần biết địa chỉ ở Porto |

---

## 7. Streaming

| Hạng mục | Session 9 | Final | Lý do |
|---|---|---|---|
| Tên topic | `porto.gps.pings` | `gps-segments` | Thống nhất tên với sơ đồ |
| Cửa sổ | Tumbling 15 phút, chế độ **update** | Trượt **15 phút, bước 5 phút**, chế độ **append** | Đúng sơ đồ ("cửa sổ 15/5 phút") và đề ("lấy cửa sổ live **đã đóng** gần nhất"): append chỉ ghi cửa sổ đã đóng |
| `maxOffsetsPerTrigger` | 500 (consumer bị lag: ~100 tin/s so với producer ~180 tin/s) | 5.000 (không lag) | Bỏ độ trễ tích lũy |
| Kết thúc phát lại | — | Gửi một message kết thúc (event time sau khung 1 giờ, tọa độ ngoài bbox) | Đẩy watermark qua mọi cửa sổ để chế độ append xuất được các cửa sổ cuối |
| Tin gửi | 42.617 | 42.395 | Kế hoạch chèn lỗi mới (cùng tỉ lệ, seed 42) |
| JSON hỏng bắt được | 78 / 78 | **81 / 81** | |
| Bản trùng bị bỏ | 394 / 394 | **413 / 413** | |
| Bản trễ bị bỏ | 186 / 189 | **208 / 223** | 15 bản lọt qua **đã đo nguyên nhân**: dữ liệu đúng giờ dừng ở 18:59:59 nên watermark dừng ở 18:49:59; cả 15 bản có event time 18:50–18:59, vẫn nằm trong watermark. `evidence/streaming/late_explained.json` |
| Đoạn đã tổng hợp | 41.022 (kỳ vọng 41.019) | 41.457 (kỳ vọng 41.442) | Chênh đúng bằng số bản trễ lọt qua (15) |
| Micro-batch | 87 | 47 | Do `maxOffsetsPerTrigger` lớn hơn |
| Theo dõi khi demo | Không có | **Mới:** trang "Giám sát streaming": đồng hồ dữ liệu, watermark, số tin bị loại theo từng lý do, bản đồ cửa sổ live, nhật ký, thanh xem lại | Người xem thấy được luồng streaming đang chạy tới đâu. Spark ghi tiến độ mỗi micro-batch vào `stream_quality` (collection đề cho phép) |

Bằng chứng: `evidence/streaming/summary.md`.

---

## 8. Mã nguồn, kiểm thử và tái lập

| Hạng mục | Session 9 | Final |
|---|---|---|
| Cấu trúc code | `src/etl/porto_etl.py`, `src/analytics/...`, `src/ml/train_travel_time.py`, `src/serving/load_mongo.py`, `src/streaming/live_zone_speed.py` | Chia theo cụm của sơ đồ: `src/core`, `batch`, `mapreduce`, `ml`, `serving`, `streaming`, `checks`; bản đồ đọc code ở `src/README.md` |
| Thư mục bằng chứng | `evidence/session9/` (A_architecture … I_readiness) | `evidence/` (A–H, streaming, red flag); mục lục `evidence/README.md` |
| Unit test | 13 | **31** (bronze, silver/gold, MapReduce khớp Spark, tìm tuyến, ML và leakage, khớp feature phục vụ/huấn luyện, ground truth producer) |
| Cấu hình | Rải rác | Tập trung ở `conf/pipeline.yaml` |
| Ảnh chụp | Chưa có (top correction #3) | Có: giao diện, YARN UI (job sample), HDFS UI, Spark UI (`evidence/screenshots/`) |

---

## 9. Tình trạng ba việc đã hứa ở Session 9 ("Top three corrections")

| # | Đã hứa | Final |
|---|---|---|
| 1 | Bỏ hoặc gắn cờ 10,22% chuyến có bước nhảy GPS, chạy lại đánh giá theo thời gian | **Đã làm**: gắn cờ `has_gps_jump`, báo cáo cả có lọc và không lọc |
| 2 | Thêm feature thời gian theo đồ thị tuyến, đồ thị chỉ xây từ train | **Đã làm**: đồ thị chỉ từ train, out-of-fold theo tuần, quyết định trên 04/2014 (giữ). Test: MAE GBT 4.192 → 4.105. `evidence/ml/graph_features.md`. Đã thử và đo; nhóm giữ model 6 feature để phục vụ vì chỉ cải thiện khoảng 2% |
| 3 | Phân công thành viên và ảnh chụp | Ảnh chụp **đã có**; phân công đang hoàn thiện |

## 10. Những gì Session 9 có mà bản final chưa có

- Bảng thiết kế triển khai cluster/cloud (G_deployment).

Cả hai đều nằm trong kế hoạch hoàn thiện trước buổi final.
