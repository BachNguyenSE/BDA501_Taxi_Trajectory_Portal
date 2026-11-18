#!/usr/bin/env bash
# Bước 6. Kafka + replay producer + Spark Structured Streaming + đối soát với manifest (src/streaming/).
# Dùng: scripts/run_streaming.sh
# Thứ tự (đề: tạo topic TRƯỚC khi chạy producer và consumer; lần trước bị race khi producer xóa topic):
#   1. dừng giao diện (đủ RAM), bật Kafka + Mongo
#   2. tạo lại topic khi CHƯA có consumer nào, xóa checkpoint và collection streaming cũ
#   3. bật Spark streaming (chạy nền), chờ sẵn sàng
#   4. chạy producer; xong thì job tự dừng khi hết dữ liệu mới
#   5. đối soát, bật lại giao diện
set -euo pipefail
cd "$(dirname "$0")/.."
EV=evidence/streaming
mkdir -p "$EV" data/stream
TOPIC=gps-segments
KT="docker compose exec -T kafka /opt/kafka/bin"

echo "== 1. Kafka + Mongo"
docker compose stop ui >/dev/null 2>&1 || true
docker compose --profile streaming up -d kafka mongo
for _ in $(seq 1 60); do $KT/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && break; sleep 2; done

echo "== 2. Tạo topic (chưa có consumer), dọn trạng thái cũ"
$KT/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$TOPIC" >/dev/null 2>&1 || true
for _ in $(seq 1 30); do $KT/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -qx "$TOPIC" || break; sleep 1; done
$KT/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$TOPIC" --partitions 3 --replication-factor 1
$KT/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic "$TOPIC" | tee "$EV/kafka_topic.txt"
rm -rf checkpoints/stream data/stream/producer_done.json data/stream/job_ready.json
docker compose exec -T mongo mongosh porto_eta --quiet --eval 'db.zone_speed_live.drop(); db.stream_quality.drop()' >/dev/null

echo "== 3. Spark Structured Streaming (nền)"
docker rm -f bda501-stream >/dev/null 2>&1 || true
docker compose run -d --name bda501-stream --no-deps --service-ports spark python src/streaming/stream_job.py >/dev/null
for _ in $(seq 1 180); do [[ -f data/stream/job_ready.json ]] && break; sleep 2; done
[[ -f data/stream/job_ready.json ]] || { docker logs bda501-stream | tail -30; echo "Spark streaming chưa sẵn sàng" >&2; exit 1; }

echo "== 4. Replay producer"
docker compose run --rm --no-deps spark python src/streaming/producer.py
echo "   chờ Spark xử lý hết và tự dừng..."
docker wait bda501-stream >/dev/null
docker logs bda501-stream 2>&1 | grep -v "^\s*$" | grep -E "STREAM_JOB|ERROR|Exception" | cut -c1-400 > "$EV/stream_job.log" || true
docker rm bda501-stream >/dev/null

echo "== 5. Đối soát"
$KT/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$TOPIC" | tee "$EV/kafka_offsets.txt"
docker compose run --rm --no-deps spark python src/streaming/reconcile.py
docker compose up -d ui
echo "Giao diện: http://localhost:8501 (lớp live đã có dữ liệu)"
