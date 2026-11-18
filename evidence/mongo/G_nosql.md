# G. NoSQL: MongoDB (số đo thật)

Nguồn: `G_nosql.json` (`src/serving/job_load_mongo.py`). MongoDB 8.0, một node trong Docker.

## Vì sao dùng MongoDB (không phải "vì nhanh hơn")

Giao diện cần **tra cứu theo khóa** rất nhiều lần cho mỗi yêu cầu: tốc độ của (zone, giờ, loại ngày), các cạnh đi ra từ một zone, cửa sổ live mới nhất. Kho Parquet hợp cho quét và tổng hợp lớn, nhưng không có index tra một dòng. MongoDB cho phép (1) index đúng theo các mẫu truy vấn đó, (2) **hai luồng cùng ghi vào một nơi**: batch ghi `zones`, `zone_hour_speed`, `zone_graph_edges`, còn streaming ghi `zone_speed_live`, `stream_quality` vào collection riêng, không ghi đè lên nhau, (3) upsert idempotent theo `_id` cố định.

## Dữ liệu đã nạp

| Collection | Trong Mongo | Trong gold | `_id` |
|---|---|---|---|
| zones | 2,142 | 2,142 | `zone_id` |
| zone_hour_speed | 72,142 | 72,142 | `zone|hour|is_weekend` |
| zone_graph_edges | 9,444 | 9,444 | `zone_from|zone_to` |

Khớp: có. Nạp bằng upsert theo `_id`, nên chạy lại nhiều lần vẫn cùng kết quả.

## Index theo access pattern

- `zone_hour_speed`: `(zone_id, hour, is_weekend)`, unique.
- `zone_graph_edges`: `(zone_from)`.
- `zones`: `(gx, gy)`, unique.
- `zone_speed_live`: `(window_end giảm dần, zone_id)`. Index tạo sẵn cho Phase 6.

## explain(): truy vấn một điểm `{"zone_id": 1574, "hour": 3, "is_weekend": false}`

| | Có index | Buộc quét cả collection (`hint $natural`) |
|---|---|---|
| Stage | IXSCAN → FETCH | COLLSCAN |
| Tài liệu đã đọc | 1 | 72,142 |

## Độ trễ đọc một điểm

| | n | p50 | p95 | max |
|---|---|---|---|---|
| Có index | 1000 | 0.121 ms | 0.146 ms | 0.313 ms |
| Buộc quét cả collection | 200 | 14.948 ms | 15.415 ms | 16.206 ms |

Độ trễ đo từ container spark tới container mongo trên cùng máy, gồm cả vòng mạng Docker. Khóa tra cứu chọn ngẫu nhiên (seed 42) trong các dòng có thật; có 50 lần đọc khởi động không tính.
