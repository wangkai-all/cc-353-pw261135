#!/usr/bin/env bash
# 启动整套演示系统：限速来源 + 入库端 + 调度器(含Web UI) + 两个采集进程
cd "$(dirname "$0")"
mkdir -p data logs

if [ -f data/source.pid ] && kill -0 "$(cat data/source.pid)" 2>/dev/null; then
  echo "已在运行？先执行 ./stop_all.sh"; exit 1
fi

nohup python3 src/source_server.py --port 9000 --db data/source.db \
  > logs/source.log 2>&1 & echo $! > data/source.pid
sleep 0.5
nohup python3 src/ingestor.py --port 8100 --db data/ingest.db \
  > logs/ingestor.log 2>&1 & echo $! > data/ingestor.pid
sleep 0.5
nohup python3 src/scheduler.py --port 8000 --db data/scheduler.db --seed \
  > logs/scheduler.log 2>&1 & echo $! > data/scheduler.pid
sleep 1
nohup python3 src/collector.py --id A --egress 127.0.0.1 --db data/collector_A.db \
  > logs/collector_A.log 2>&1 & echo $! > data/collector_A.pid
nohup python3 src/collector.py --id B --egress 127.0.0.2 --db data/collector_B.db \
  > logs/collector_B.log 2>&1 & echo $! > data/collector_B.pid

echo "已启动："
echo "  来源     http://127.0.0.1:9000  (admin: /admin/limits /admin/requests)"
echo "  调度器   http://127.0.0.1:8000  (Web UI)"
echo "  入库端   http://127.0.0.1:8100"
echo "  采集进程 A=127.0.0.1  B=127.0.0.2"
echo "日志在 logs/，停止用 ./stop_all.sh"
