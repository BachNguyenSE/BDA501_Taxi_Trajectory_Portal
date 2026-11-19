# Hướng dẫn kiến trúc: Porto Taxi ETA

Đi kèm sơ đồ [architecture.svg](architecture.svg). Số trong vòng tròn của sơ đồ trùng với số mục ở đây.

> **Trạng thái hiện tại: đã chạy mục 2-10 và 12-14 (Phase 1-6); mục 11 (nguồn GPS live thật) chỉ là thiết kế.** Chi tiết ở bảng "Trạng thái và bằng chứng" cuối file. Mọi con số trong tài liệu này hoặc là yêu cầu của đề, hoặc là số tham chiếu lần trước (chưa đo lại), hoặc là số đã đo thật (chỉ liệt kê ở mục "Trạng thái và bằng chứng" cuối file).

Quy ước theo đề: **[I]** là phần *sẽ* implement và chạy (chỉ được gọi là "đã làm" khi có file trong `evidence/`), **[P]** là phần chỉ thiết kế, không code. Màu cam là luồng batch, màu xanh là luồng streaming.

## 0. Bức tranh chung

Bài toán là *known-destination ETA*. Người dùng nhập điểm A, điểm B và giờ xuất phát. Hệ thống trả về thời gian dự kiến, các vùng có khả năng tắc và tuyến gợi ý.

Cách tổ chức: **batch học từ lịch sử**, **streaming cho biết "bây giờ có bất thường không"**, và hai luồng gặp nhau ở MongoDB trước khi lên giao diện.

```
BATCH      1 → 2 → 3 ⇄ 5 → 9 → 10
                3 → 6 ⇢ 5            (đối chiếu)
                3 → 7 → 8 → 10
                4 → 3                (tham chiếu)
STREAMING  11 ⇢ 12 → 13 → 14 → 9 → 10
                3 ─raw─▶ 12
```

Năm cụm trong sơ đồ:

| Cụm | Các mục | Vai trò |
|---|---|---|
| Nguồn | 1, 11 | Dữ liệu từ bên ngoài vào |
| Thu nhận | 2, 4, 12 | Đưa dữ liệu vào hệ thống, chuẩn hóa bước đầu |
| Lưu trữ | 3, 8, 13 | Nơi dữ liệu và model nằm lại |
| Xử lý | 5, 6, 7, 14 | Tính toán, huấn luyện |
| Phục vụ | 9, 10 | Kết quả đến tay người dùng |

---

## Mục 1. Kaggle · Porto `[I]`

- **Làm gì:** Nguồn dữ liệu duy nhất, `train.csv`. Gồm 1,71 triệu chuyến taxi ở Porto, 01/07/2013 đến 30/06/2014, mỗi chuyến là chuỗi điểm GPS cách nhau 15 giây.
- **Flow:** 1 → 2. Có script tải riêng, file thô không commit lên git.
- **Tại sao:** Đây là bộ duy nhất trong các ứng viên có quỹ đạo GPS thật. NYC TLC Yellow Taxi đã bị loại vì chỉ có mã zone, không có quỹ đạo, nên không tính được tốc độ theo vùng. Báo cáo cần nêu lý do này.

## Mục 2. Tải + bronze `[I]`

- **Làm gì:** Đọc CSV, parse cột `POLYLINE` (chuỗi JSON `[[lon, lat], ...]`, lon đứng trước). Mỗi dòng gắn thêm `reject_reason`: `missing_data`, `too_few_points`, `too_long`, `bad_json`, hoặc null nếu hợp lệ.
- **Flow:** 1 → 2 → 3 (ghi vào tầng raw và bronze).
- **Tại sao:** Bronze **giữ mọi dòng** thay vì xóa dòng xấu. Nhờ vậy đếm được mỗi lý do loại bỏ bao nhiêu dòng, và con số này dùng để đối soát với MapReduce (mục 6). Nếu xóa im lặng thì không chứng minh được dữ liệu sạch.

- **Thực tế đã làm:** `train.csv` được đặt vào `data/raw/` thủ công; script tải (`scripts/download_data.sh`) mới là khung, chưa implement vì cần tài khoản Kaggle.
- **Phát hiện ở Phase 2:** `TRIP_ID` không duy nhất (cùng xe, cùng giây xuất phát). Bronze thêm khóa `trip_uid` = `TRIP_ID` + md5 của polyline; mọi bước ghép điểm theo chuyến dùng `trip_uid`.

## Mục 3. Data lake · Parquet `[I]`

- **Làm gì:** Kho trung tâm, chia 4 tầng:

  | Tầng | Nội dung |
  |---|---|
  | raw | CSV gốc, không sửa |
  | bronze | Đã parse, giữ mọi dòng kèm lý do lỗi |
  | silver | Chuyến và đoạn 15 giây sạch (đo được 79.502.899 đoạn) |
  | gold | Bảng tổng hợp: `zones`, `zone_hour_speed`, `zone_graph_edges`, `trip_features` |

- **Flow:** Nhận từ 2 và 4. Spark batch (5) đọc và ghi hai chiều. MapReduce (6) đọc raw. Spark ML (7) đọc gold. Replay producer (12) đọc raw.
- **Tại sao:** Tách tầng để mỗi tầng có một mức độ tin cậy rõ ràng và có thể chạy lại từ tầng trước mà không phải tải lại. Parquet là định dạng cột nên đọc nhanh và nén tốt. Lưu trên Docker volume ở máy đơn. HDFS hoặc S3 là phương án khi chạy cụm **[P]**.

## Mục 4. holidays.csv `[I]`

- **Làm gì:** Bảng 10 ngày, 15/08/2013 đến 24/06/2014: 9 ngày lễ quốc gia Bồ Đào Nha cộng São João (24/06, lễ địa phương của Porto; ngoài đề, **đã được duyệt**; đề chỉ nói 10 ngày lễ quốc gia). Bốn ngày lễ bị Bồ Đào Nha bỏ từ 2013 đến 2015 (Corpus Christi, 5/10, 1/11, 1/12) không có trong bảng. Dùng để tạo feature `is_holiday`.
- **Flow:** 4 → 3. Spark batch (5) join vào `trip_features` theo ngày địa phương.
- **Tại sao:** Cột `DAY_TYPE` trong dữ liệu gốc toàn là `A`, nên không dùng được. Ngày lễ ảnh hưởng đến giao thông nên cần có. Danh sách này **tự tổng hợp từ lịch ngày lễ chính thức**, không phải lấy từ dataset. Báo cáo phải nói rõ điều này.

## Mục 5. Spark batch `[I]`

- **Làm gì:**
  - Làm sạch: bỏ dòng lỗi, bỏ đoạn có tốc độ trên 150 km/h.
  - Tách đoạn: explode `POLYLINE` thành điểm, dùng window `lag` theo chuyến để ghép thành đoạn 15 giây. Zone của đoạn là zone của trung điểm, giờ là giờ địa phương (`Europe/Lisbon`) lúc bắt đầu đoạn.
  - Tính các bảng gold và feature.
  - Spark SQL: ít nhất 2-3 truy vấn có ý nghĩa, in `EXPLAIN FORMATTED`.
  - Thí nghiệm hiệu năng: chạy cùng một query bằng BroadcastHashJoin và SortMergeJoin, so thời gian và lượng shuffle.
- **Flow:** 3 ⇄ 5 (đọc bronze, ghi silver và gold). Sau đó 5 → 9 (đưa gold vào MongoDB). 6 đối chiếu ngược lại 5.
- **Tại sao:** Dữ liệu có ~83 triệu điểm GPS, pandas một máy không xử lý nổi. Không được lọc bbox trước khi `lag`, vì lọc trước sẽ ghép hai điểm không liền nhau thành một đoạn sai. Máy chỉ có ~8 GB RAM nên phải select đúng cột cần và dùng `persist(DISK_ONLY)`, không `cache()` toàn bộ.

- **Thực tế đã làm:** một chuyến bị ghi lặp 3 lần (cùng `trip_uid`); silver giữ một bản, bronze vẫn giữ đủ. Đoạn được đối soát với một cách đếm độc lập (khớp tuyệt đối).

## Mục 6. MapReduce `[I]`

- **Làm gì:** Hadoop Streaming trên YARN + HDFS. Mapper, combiner, reducer viết bằng Python. Đọc **raw CSV**, tính tốc độ theo key (zone, giờ). Reducer chỉ cộng dồn `total_km`, `total_s`, `n_segments`.
- **Flow:** 3 (raw, copy sang HDFS) → 6 ⇢ 5. Kết quả so từng key với một query Spark SQL cùng logic. Không ghi vào MongoDB và không phục vụ API.
- **Tại sao:** Làm vai **đối chiếu độc lập**. Hai công cụ khác nhau tính cùng một thứ mà ra cùng kết quả thì mới tin được pipeline. Reducer chỉ cộng dồn, không tính trung bình của trung bình, nên dùng lại được làm combiner và giảm mạnh lượng dữ liệu shuffle. Sai số km cho phép là `n_segments × 1e-6`, do làm tròn khi ghi ra text. Hadoop và Spark không chạy cùng lúc vì không đủ RAM, nên đây là profile `hadoop` riêng.

## Mục 7. Spark ML `[I]`

- **Làm gì:** Dự đoán `duration_min` bằng Linear Regression (baseline) và GBT Regressor. Feature chỉ gồm thông tin có trước khi xuất phát: `straight_km`, `bearing_deg`, `hour`, `day_of_week`, `is_weekend`, `is_holiday`. Hai baseline bắt buộc: luôn đoán trung bình, và khoảng cách chia tốc độ lịch sử.
- **Flow:** 3 (gold) → 7 → 8.
- **Tại sao:**
  - Đánh giá bằng **temporal hold-out**: train trước 05/2014, test 05-06/2014. Random split sẽ để model nhìn thấy tương lai, làm kết quả đẹp giả.
  - Không dùng `zone_hour_speed` tính trên toàn bộ dữ liệu làm feature, vì như vậy là leakage. Nếu thêm feature từ đồ thị tuyến thì đồ thị chỉ xây từ dữ liệu train.
  - Nên lọc hoặc gắn cờ khoảng 10% nhãn bị mất GPS.
  - Metric: MAE, RMSE, R², signed error.
- **Thiết kế mở rộng (đề mục 7, cải tiến 5), chưa có bằng chứng:** thêm feature từ tuyến Dijkstra trên đồ thị zone (`route_km`, `graph_time_min`, `n_zones`, `detour_ratio`), đồ thị chỉ xây từ dữ liệu train. Để feature của chính các chuyến train không bị lộ (mỗi chuyến đã góp cạnh vào đồ thị), đồ thị cho chuyến train phải xây từ các chuyến khác, ví dụ theo nhóm tuần. Trọng số cạnh mặc định là thời gian đi qua (đúng đề); **phạt theo tần suất cạnh là đề xuất ngoài đề, chờ duyệt**. Mục tiêu "R² cao hơn 0,24" chỉ được ghi là đạt khi có số đo thật.

## Mục 8. models/ `[I]`

- **Làm gì:** Lưu model đã huấn luyện kèm `features.json` (tên và thứ tự feature).
- **Flow:** 7 → 8 → 10.
- **Tại sao:** Tách huấn luyện khỏi phục vụ. Giao diện nạp model mà không cần khởi động Spark. Khi nạp phải kiểm tra thứ tự feature, nếu sai thì dự đoán sẽ lệch mà không báo lỗi. Nếu dùng bản sklearn để phục vụ thì phải nói rõ trong báo cáo. Model phục vụ cần được huấn luyện trên giai đoạn train của temporal split, để thử nghiệm chạy một chuyến thật là ngoài mẫu.

## Mục 9. MongoDB `[I]`

- **Làm gì:** Nơi cả hai luồng cùng ghi vào, nhưng vào collection **riêng**:
  - Batch: `zones`, `zone_hour_speed`, `zone_graph_edges`
  - Streaming: `zone_speed_live`, `stream_quality`
- **Flow:** 5 → 9 (gold, batch). 14 → 9 (live, streaming). 9 → 10 (giao diện đọc).
- **Tại sao:** Giao diện cần tra cứu theo điểm rất nhiều lần ("zone này, giờ này, tốc độ bao nhiêu"). Dạng truy vấn đó hợp với kho có index hơn là quét file Parquet. Phải chứng minh bằng `explain()` thấy IXSCAN và đo độ trễ đọc một điểm, không chọn NoSQL chỉ vì "nhanh hơn". Streaming ghi vào collection riêng để không ghi đè bảng batch. Ghi phải idempotent: upsert với `_id = zone|window_start`, để chạy lại không sinh bản trùng.

## Mục 10. Streamlit UI `[I]`

- **Làm gì:**
  - Chọn A, B và giờ xuất phát trên bản đồ.
  - Tính ETA bằng model từ `models/`.
  - Chạy Dijkstra trên đồ thị zone (trọng số là thời gian đi qua cạnh) để tìm tuyến.
  - Vẽ tuyến lên **lưới zone dạng sơ đồ**, tô màu zone theo mức tắc, viền riêng các zone đang chậm hơn bình thường.
- **Flow:** 8 (model) và 9 (tốc độ lịch sử, đồ thị, dữ liệu live) → 10.
- **Tại sao:** Streamlit viết nhanh bằng Python, hợp với bài mà phần trọng tâm là xử lý dữ liệu lớn. Lớp live **chỉ đánh dấu, không đổi ETA**. Lấy cửa sổ live đã đóng gần nhất, so với tốc độ lịch sử cùng zone và giờ, tỉ lệ dưới 0,7 thì đánh dấu là chậm. Làm vậy vì model được huấn luyện trên dữ liệu lịch sử, trộn tín hiệu live vào ETA sẽ làm model mất tính nhất quán.
- **Bám đề:** đề ghi "sơ đồ ghi Streamlit thì dùng Streamlit", sơ đồ đã duyệt ghi Streamlit nên dùng Streamlit. Logic `estimate(origin, destination, departure_time)` đặt trong một hàm riêng, trả về đúng 4 trường của đề (`predicted_duration_min`, `route`, `congested_zones`, `live`) để Streamlit gọi. Nếu cần REST (`POST /estimate`) thì bọc hàm này bằng FastAPI, không đổi logic. Việc có làm REST hay không đang chờ anh quyết.

## Mục 11. Taxi GPS live `[P]`

- **Làm gì:** Nguồn GPS thời gian thực thật. **Không có**, chỉ thiết kế.
- **Flow:** 11 ⇢ 12 (mũi tên nét đứt: sẽ thay thế replay producer).
- **Tại sao:** Giữ cùng schema message với replay producer, để khi có nguồn thật chỉ cần thay producer, phần Kafka trở xuống không đổi. Báo cáo không được gọi bản replay là real-time.

## Mục 12. Replay producer `[I]`

- **Làm gì:** Đọc raw rồi phát vào Kafka theo thứ tự thời gian sự kiện, tốc độ ×60 (1 giây thật bằng 1 phút dữ liệu). Khung mẫu: 14/05/2014, 16:00-19:00 giờ địa phương. Mỗi message là một điểm GPS kèm điểm trước đó (`prev_lon`, `prev_lat`), nên thực chất là một đoạn. `acks=all`, key là `TAXI_ID`.
- **Cố ý chèn dữ liệu xấu:** 1% bản trùng, 0,5% đến trễ 30 phút, 0,2% JSON hỏng. Số lượng thật ghi vào một manifest.
- **Flow:** 3 (raw) → 12 → 13.
- **Tại sao:** Không có nguồn live thật nên cần mô phỏng. Dữ liệu xấu có chủ đích kèm manifest cho phép **đối soát**: streaming bắt được bao nhiêu trên tổng số đã chèn. Nếu không biết chính xác đã chèn gì thì không đánh giá được khả năng xử lý lỗi.

## Mục 13. Kafka `[I]`

- **Làm gì:** Hàng đợi message. Topic `gps-segments`, 3 partition, KRaft một node trong Docker, key là `TAXI_ID`.
- **Flow:** 12 → 13 → 14.
- **Tại sao:** **Không phải vì throughput.** 448 xe chỉ khoảng 30 msg/s, thừa sức cho cách đơn giản hơn. Lý do thật:
  - Tách producer khỏi consumer.
  - Đọc lại được theo offset và checkpoint.
  - Nhiều consumer độc lập cùng đọc một topic (hai query của mục 14).
  - Key là `TAXI_ID` giữ thứ tự theo từng xe.
- **Lưu ý:** Tạo topic **trước** khi chạy producer và consumer. Lần trước bị race khi producer xóa topic lúc consumer đang đọc.

## Mục 14. Structured Streaming `[I]`

- **Làm gì:** Hai query trên cùng một topic, trigger 5 giây, có checkpoint và `maxOffsetsPerTrigger`:
  - **Quality:** đếm JSON hỏng, ghi vào `stream_quality`.
  - **Live:** parse → `withWatermark(event_time, 10 phút)` → `dropDuplicatesWithinWatermark(trip_id, seq)` (lưu ý: `TRIP_ID` không duy nhất; kiểm tra ở Phase 6 xem khung replay có chuyến nào trùng ID không) → lọc bbox và tốc độ → `window(15 phút, trượt 5 phút)` theo zone → `foreachBatch` upsert vào MongoDB.
- **Flow:** 13 → 14 → 9.
- **Tại sao:** Watermark cho biết chờ dữ liệu đến trễ bao lâu rồi mới chốt cửa sổ. Dedup xử lý bản trùng. Hai query tách riêng vì mục đích khác nhau (đo chất lượng và tính tốc độ). Đối soát với manifest của producer: số message, JSON hỏng, bản trùng bị bỏ, bản trễ bị bỏ, số đoạn đã tổng hợp. Một số bản trễ lọt qua là bình thường: từ Spark 3.5, việc lọc dùng watermark của batch trước, và Spark chỉ đảm bảo không bỏ dòng nằm trong watermark.

---

## Phần [P] gộp lại

Các phần này chỉ thiết kế, ghi rõ trong báo cáo, không trình bày như đã làm:

- Nguồn GPS live thật (mục 11)
- Lịch ETL hằng ngày và huấn luyện lại hằng tháng
- HDFS hoặc S3 làm kho cho chạy cụm (HDFS chỉ dùng thật cho job MapReduce)
- Triển khai cụm hoặc cloud

## Chạy theo profile Docker

| Profile | Gồm | Mục |
|---|---|---|
| mặc định | Spark, MongoDB, Streamlit | 2, 3, 5, 7, 8, 9, 10 |
| `hadoop` | HDFS + YARN | 6 |
| `streaming` | Kafka | 12, 13, 14 |

Không chạy Hadoop và Spark cùng lúc vì không đủ RAM (~8 GB).

---

## Trạng thái và bằng chứng

Cập nhật mục này mỗi khi xong một phase. Chỉ được chuyển một mục sang "đã chạy" khi có file tương ứng trong `evidence/`.

| Mục | Trạng thái | Bằng chứng |
|---|---|---|
| 1 Kaggle · Porto | Có file `train.csv` trong `data/raw/` | `evidence/eda/initial_profile.md` (đo sơ bộ) |
| 2 Tải + bronze | Đã chạy sample và full (Phase 1) | `evidence/bronze/phase1_summary.md`, `reject_counts_full.json` |
| 3 Data lake | Đủ raw, bronze, silver, gold (Parquet) | `evidence/bronze/`, `evidence/spark/D_counts_full.json` |
| 4 holidays.csv | Đã tạo file (10 ngày, tự tổng hợp), kiểm tra ngày hợp lệ | `evidence/bronze/phase1_summary.md`; chưa đối chiếu nguồn chính thức |
| 5 Spark batch | Đã chạy sample và full (Phase 2) | `evidence/spark/phase2_summary.md`, `B_benchmark.md`, `C_query_plan_explained.md` |
| 6 MapReduce | Đã chạy sample và full trên YARN + HDFS (Phase 3), khớp Spark từng key | `evidence/mapreduce/phase3_summary.md`, `reconcile_full.json` |
| 7 Spark ML | Đã chạy sample và full (Phase 4): GBT MAE 4,21 phút, temporal hold-out | `evidence/ml/E_model_validation.md`, `leakage_checks_full.json` |
| 8 models/ | Đã lưu model Spark và `features.json` | `evidence/ml/E_model_validation.md` |
| 9 MongoDB | Đã nạp 3 collection batch, index, IXSCAN, độ trễ p50 0,12 ms (Phase 5) | `evidence/mongo/G_nosql.md` |
| 10 Streamlit UI + REST API | Đã chạy (Phase 5): REST `POST /estimate` p50 6.44 ms; 3 cách nhập, tuyến trên lưới zone, F3 end-to-end. Lớp live chờ Phase 6 | `evidence/serving/F3_end_to_end.md`, `evidence/screenshots/` |
| 11 Taxi GPS live | Chỉ thiết kế [P]; producer giữ cùng schema message để thay được | không cần |
| 12 Replay producer | Đã chạy (Phase 6): 41.981 message gốc, chèn 413 trùng, 223 trễ, 81 JSON hỏng, có manifest | `evidence/streaming/producer_manifest.json` |
| 13 Kafka | Đã chạy: KRaft một node, topic 3 partition, tạo trước producer và consumer | `evidence/streaming/kafka_topic.txt`, `kafka_offsets.txt` |
| 14 Structured Streaming | Đã chạy: 2 query, trang "Giám sát streaming" theo dõi trực tiếp, đối soát khớp mọi dòng trừ 15 bản trễ (đã giải thích bằng số đo) | `evidence/streaming/summary.md` |

**Đề xuất ngoài đề đã được duyệt:** São João trong `holidays.csv`; phạt theo tần suất cạnh khi chọn tuyến (mặc định tắt); báo cáo cả có lọc và không lọc nhãn nhiễu; gắn cờ nhiễu GPS trên chuyến; giữ một bản cho chuyến ghi lặp.

**Đã quyết sau Phase 3:**
- Zone 2685 gần sân bay (1,65 km/h giờ cao điểm, 91,9% đoạn đứng yên): giữ đúng đề, ghi trong báo cáo là khu xe đứng chờ chứ không phải tắc đường. Đề xuất "bỏ đoạn đứng yên đầu chuyến" đã rút lại vì số đo cho thấy không có tác dụng.
- Tìm đường bỏ cạnh dưới 5 lượt (`routing.min_edge_transitions`). Bằng chứng: `evidence/spark/routing_check.md`.

- Thời gian cạnh khi tìm đường tính từ tốc độ zone × giờ × loại ngày (`routing.edge_time`), thay vì tốc độ của riêng các đoạn cắt ranh giới. Tỉ lệ thời gian tuyến / thực tế tăng từ 0.34 lên 0.71.

- Khoảng ước lượng 80% cho ETA (mở rộng ngoài đề, đã duyệt): hiệu chỉnh trên sai số 04/2014, bao phủ 80.3% trên test. `estimate()` trả thêm `predicted_interval_min`.

- REST `POST /estimate` (FastAPI, `src/serving/api.py`, service `api` cổng 8000) bọc cùng hàm `estimate()` với giao diện; kiểm tra thứ tự feature khi khởi động; p50 6.44 ms. Bằng chứng: `evidence/serving/api.md`.

**Lựa chọn thiết kế ở Phase 5 (đề không quy định chi tiết):** mức tắc của zone = tốc độ zone tại giờ đó / tốc độ cả ngày của chính zone (< 0,7 là tắc, < 0,9 là đông, cấu hình `ui`; dùng chữ "đông" để không lẫn với "chậm bất thường" của lớp live). Điểm A, B nằm ngoài mạng chính thì gắn vào zone gần nhất trong mạng chính. Danh sách 15 địa điểm có sẵn dùng tọa độ xấp xỉ, đã kiểm tra bằng dữ liệu.

**Số liệu lấy từ lần trước, chỉ để so sánh cho hợp lý, không dùng làm kết quả:** ~80 triệu đoạn, 40.618 key MapReduce, MAE 4,02 phút, R² ~0,24, 42.617 message streaming.
