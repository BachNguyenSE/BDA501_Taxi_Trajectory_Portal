# Streaming: đối soát với manifest của producer (số đo thật)

Chạy bằng `scripts/run_streaming.sh`. Khung phát lại: 2014-05-14 16:00 – 2014-05-14 19:00 (giờ Lisbon), tốc độ ×60, phát lại mất 210.0 giây thật. Đây là **bản phát lại dữ liệu lịch sử, không phải real-time**.

Kafka 3.9 KRaft một node, topic `gps-segments` 3 partition, key = TAXI_ID, `acks=all`. Message theo partition: p0: 14,664, p1: 13,596, p2: 14,135. Lỗi gửi: 0.

Spark Structured Streaming: trigger 5 seconds, watermark 10 minutes, `dropDuplicatesWithinWatermark('trip_id', 'seq')`, cửa sổ 15 minutes trượt 5 minutes, `maxOffsetsPerTrigger=5000`, có checkpoint. Query live ghi ở chế độ append (chỉ cửa sổ đã đóng) bằng `foreachBatch` upsert `_id = zone|window_start`.

## Bảng đối soát

| Hạng mục | Producer (manifest) | Spark / Mongo | Nguồn số phía Spark |
|---|---|---|---|
| Message gửi vào Kafka | 42,395 | 42,395 | offset cuối Kafka (3 partition) |
| Message Spark đọc (query quality) | 42,395 | 42,395 | stream_quality |
| Message Spark đọc (query live) | 42,395 | 42,395 | progress |
| JSON hỏng bắt được | 81 | 81 | stream_quality |
| Bản trùng bị bỏ | 413 | 413 | numDroppedDuplicateRows |
| Bản trễ bị bỏ (watermark) | 223 | 208 | numRowsDroppedByWatermark |
| Đoạn đã tổng hợp | 41,442 | 41,457 | tổng n_segments / 3 trong zone_speed_live |

Ghi chú:
- Producer gửi 41,981 message gốc. Số phận mỗi message: normal 41,264, late 223, duplicate 413, corrupt 81. Thêm 413 bản sao và 1 message kết thúc (event_time sau khung 1 giờ, tọa độ ngoài bbox, dùng để đẩy watermark cho Spark xuất các cửa sổ cuối).
- "Đoạn đã tổng hợp" phía producer là số đoạn đúng giờ, JSON hợp lệ, qua bộ lọc bbox và tốc độ (bản sao chỉ tính một lần), **giả sử mọi bản trễ đều bị bỏ**. Mỗi đoạn thuộc đúng 3 cửa sổ trượt nên phía Mongo chia tổng cho 3.
- Bản trễ **không** bị bỏ: 15. Chênh lệch đoạn đã tổng hợp so với kỳ vọng: 15. Nếu có bản trễ lọt qua thì đó là hành vi đã biết của Spark: watermark dùng để lọc là watermark của batch **trước**, và Spark chỉ cam kết không bỏ dòng nằm trong watermark, không cam kết bỏ mọi dòng trễ.
- `TRIP_ID` dùng chung giữa nhiều dòng trong khung phát lại: 0 (không ảnh hưởng tới khóa bỏ trùng `(trip_id, seq)`).
- `zone_speed_live`: 9,658 tài liệu (zone × cửa sổ), cửa sổ từ 2014-05-14 15:50:00 đến 2014-05-14 19:10:00 (giờ Lisbon). Streaming chỉ ghi collection riêng, không ghi đè bảng batch.
