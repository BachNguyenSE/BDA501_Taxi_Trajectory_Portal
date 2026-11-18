#!/usr/bin/env bash
# Bước 3. MapReduce (Hadoop Streaming) trên YARN + HDFS, rồi đối chiếu từng key với Spark SQL (src/mapreduce/job_mapreduce_reconcile.py).
# Dùng: scripts/run_mapreduce.sh [--full]   (mặc định: sample 20.000 dòng đầu của train.csv)
# KEEP_HADOOP=1 scripts/run_mapreduce.sh   giữ Hadoop chạy sau job để chụp YARN UI
# Không chạy cùng Spark (không đủ RAM): script dừng Spark trước, dừng Hadoop sau khi lấy kết quả.
set -euo pipefail
cd "$(dirname "$0")/.."
MODE=sample
[[ "${1:-}" == "--full" ]] && MODE=full
EV=evidence/mapreduce
mkdir -p "$EV" "data/mr/$MODE"
JAR=/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.5.0.jar

echo "== 1. Tham số mapper từ conf/pipeline.yaml"
docker compose run --rm --no-deps spark python src/mapreduce/export_mr_conf.py data/mr/mr_conf.json
conf() { python3 -c "import json;print(json.load(open('data/mr/mr_conf.json'))['$1'])"; }
HDFS_IN="$(conf hdfs_input)/$MODE"; HDFS_OUT="$(conf hdfs_output)/$MODE"; REDUCERS="$(conf reducers)"

echo "== 2. Dữ liệu vào ($MODE)"
if [[ $MODE == sample ]]; then
  IN=data/sample/raw/train_sample.csv
  mkdir -p data/sample/raw
  head -n 20001 data/raw/train.csv > "$IN"          # header + 20.000 dòng
else
  IN=data/raw/train.csv
fi

echo "== 3. Dừng Spark, khởi động HDFS + YARN"
docker compose stop spark >/dev/null 2>&1 || true
docker compose --profile hadoop up -d namenode datanode resourcemanager nodemanager

echo "== 4. Chờ YARN có NodeManager (poll REST cổng 8088, không dùng 'yarn node -list' vì lệnh này treo)"
n=0
for _ in $(seq 1 120); do
  n=$(curl -s http://localhost:8088/ws/v1/cluster/metrics \
      | python3 -c "import sys,json;print(json.load(sys.stdin)['clusterMetrics']['activeNodes'])" 2>/dev/null || echo 0)
  [[ "$n" -ge 1 ]] && break
  sleep 2
done
[[ "$n" -ge 1 ]] || { echo "YARN chưa có NodeManager nào hoạt động" >&2; exit 1; }
docker compose exec -T namenode hdfs dfsadmin -safemode wait

NM="docker compose exec -T nodemanager"
echo "== 5. Nạp raw CSV vào HDFS"
$NM hdfs dfs -mkdir -p "$HDFS_IN"
$NM hdfs dfs -put -f "/workspace/$IN" "$HDFS_IN/train.csv"
$NM hdfs dfs -rm -r -f "$HDFS_OUT"

echo "== 6. Chạy Hadoop Streaming"
START=$(date +%s)
$NM hadoop jar "$JAR" \
  -D mapreduce.job.name="porto-zone-hour-$MODE" \
  -D mapreduce.job.reduces="$REDUCERS" \
  -files /workspace/src/mapreduce/mapper.py,/workspace/src/mapreduce/reducer.py,/workspace/src/mapreduce/mr_common.py,/workspace/data/mr/mr_conf.json \
  -mapper "python3 mapper.py" \
  -combiner "python3 reducer.py" \
  -reducer "python3 reducer.py" \
  -input "$HDFS_IN/train.csv" \
  -output "$HDFS_OUT" 2>&1 | tee "$EV/job_$MODE.log"
END=$(date +%s)
echo "{\"wall_seconds_submit_to_finish\": $((END - START))}" > "$EV/wall_$MODE.json"

echo "== 7. Lấy kết quả và thông tin job từ YARN REST"
$NM hdfs dfs -cat "$HDFS_OUT/part-*" > "data/mr/$MODE/mr_zone_hour.tsv"
curl -s "http://localhost:8088/ws/v1/cluster/apps?states=FINISHED" > "$EV/yarn_apps_$MODE.json"

if [[ "${KEEP_HADOOP:-0}" == "1" ]]; then
  echo "== KEEP_HADOOP=1: giữ Hadoop chạy để chụp YARN UI (http://localhost:8088), bỏ qua bước đối chiếu"
  exit 0
fi
echo "== 8. Dừng Hadoop, đối chiếu bằng Spark"
docker compose --profile hadoop stop namenode datanode resourcemanager nodemanager
docker compose run --rm --no-deps spark python src/mapreduce/job_mapreduce_reconcile.py --mode "$MODE"
