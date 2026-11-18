"""streaming: mục 12-14 của sơ đồ. Phát lại GPS ×60 qua Kafka, Spark Structured Streaming ghi MongoDB.

producer.py    đọc raw, gửi Kafka theo thời gian sự kiện, cố ý chèn trùng / trễ / JSON hỏng, ghi manifest
stream_job.py  2 query: chất lượng (đếm JSON hỏng) và live (watermark, bỏ trùng, cửa sổ 15/5 phút theo zone)
reconcile.py   đối soát số Spark bắt được với manifest của producer
Chạy cả chuỗi: scripts/run_streaming.sh. Đây là bản phát lại, KHÔNG phải real-time.
"""
