# C. Query plan: EXPLAIN FORMATTED và giải thích

Plan đầy đủ: `C_query_plan_full.txt` (in bằng `EXPLAIN FORMATTED`, Spark 4.0.1, chạy local[*], 48 shuffle partition). Số trong ngoặc là số node trong plan.

## Q1: zone chậm nhất giờ cao điểm (ngày thường, 8-9h và 17-19h)

`zone_hour_speed JOIN zones`, lọc rồi gom theo zone, lấy 10 zone chậm nhất.

- **Lọc đẩy xuống nguồn (1):** `PushedFilters: [Not(EqualTo(is_weekend,true)), In(hour, [17,18,19,8,9]), ...]`. Điều kiện `WHERE` được đẩy xuống lúc đọc Parquet, nên Spark bỏ bớt dữ liệu ngay khi đọc.
- **Join (7) là BroadcastHashJoin BuildRight:** bảng `zones` (2,142 dòng) nhỏ hơn ngưỡng tự broadcast (10 MB) nên Spark tự chọn broadcast (6). Bảng lớn không phải shuffle cho phép join.
- **Exchange duy nhất (10):** `hashpartitioning(zone_id, gx, gy, center_lat, center_lon, 48)`, phục vụ `GROUP BY`. Trước đó có HashAggregate cục bộ (9) gom một phần trước khi shuffle, sau đó (11) gom phần còn lại.
- **TakeOrderedAndProject (13):** `ORDER BY ... LIMIT 10` không sắp xếp toàn bộ mà chỉ giữ top 10 trên mỗi partition, nên không cần thêm Exchange.

## Q2: tốc độ theo giờ trong ngày

- Gom hai bước: HashAggregate cục bộ (2), Exchange `hashpartitioning(hour, is_weekend, 48)` (3), rồi HashAggregate cuối (4).
- **Exchange thứ hai (5):** `rangepartitioning(is_weekend, hour)`, do `ORDER BY` trên toàn bộ kết quả cần phân vùng theo khoảng. Đây là chỗ thứ hai có shuffle, dù kết quả chỉ có 48 dòng.
- Không có join.

## Q3: join chuyến với zone × giờ tại điểm xuất phát

`trip_features JOIN zone_hour_speed ON origin_zone = zone_id AND hour AND is_weekend`, chỉ trên `split = 'train'`.

- **Lọc đẩy xuống (1):** `EqualTo(split,train)` cùng các `IsNotNull` của khóa join. Spark tự thêm `IsNotNull` vì inner join không bao giờ khớp giá trị null.
- **Join (7) là BroadcastHashJoin BuildRight:** `zone_hour_speed` (72,142 dòng) được broadcast; `trip_features` (1,673,303 dòng) không bị shuffle.
- Exchange (10) cho `GROUP BY` và Exchange (12) `rangepartitioning` cho `ORDER BY`.

## Cùng phép join, hai cách (dùng cho thí nghiệm B)

| | `/*+ BROADCAST(h) */` | `/*+ MERGE(h) */`, tắt tự broadcast |
|---|---|---|
| Join | BroadcastHashJoin BuildRight (6) | SortMergeJoin (9) |
| Exchange (shuffle) | Không có. Chỉ có BroadcastExchange (5) gửi bảng nhỏ đi | 2 Exchange: (3) `hashpartitioning(origin_zone, hour, is_weekend, 48)` cho bảng chuyến, (7) `hashpartitioning(zone_id, hour, is_weekend, 48)` cho bảng zone × giờ |
| Sort | Không | 2 Sort (4), (8) trước khi merge |

Vì sao khác nhau: sort-merge join yêu cầu hai bên cùng phân vùng và cùng sắp xếp theo khóa join, nên **cả hai bảng phải shuffle rồi sort**. Broadcast join gửi nguyên bảng nhỏ đến mọi task, bảng lớn giữ nguyên phân vùng, nên **không có shuffle**. Điều kiện để dùng được: bảng nhỏ phải vừa bộ nhớ (ở đây 72,142 dòng).

Lưu ý: plan in bằng `EXPLAIN` là plan *trước khi chạy* (`AdaptiveSparkPlan isFinalPlan=false`). AQE có thể đổi join lúc chạy. Vì vậy thí nghiệm B kiểm tra lại loại join trong **plan cuối sau khi chạy**, và ghi nó vào `B_benchmark_full.json` (`join_in_final_plan`).
