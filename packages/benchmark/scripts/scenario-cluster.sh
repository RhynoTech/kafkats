#!/usr/bin/env bash
# Start or stop the KRaft cluster the scenario benchmarks run against.
#
#   scenario-cluster.sh start <1|3>    fresh cluster (existing data is discarded)
#   scenario-cluster.sh stop <1|3|all>
#   scenario-cluster.sh status
#   scenario-cluster.sh bootstrap <1|3>
#
# Environment:
#   SCENARIO_PORT_BASE      (default 39100) 1 broker: base; 3 brokers: base+1..base+3; controllers +50
#   SCENARIO_CLUSTER_PREFIX (default kafkats-scn) containers are <prefix>-1 and <prefix>-2..4
#   SCENARIO_KAFKA_IMAGE    (default apache/kafka:3.9.1)
#   SCENARIO_KAFKA_HEAP     (default 1g)
set -euo pipefail

BASE=${SCENARIO_PORT_BASE:-39100}
PREFIX=${SCENARIO_CLUSTER_PREFIX:-kafkats-scn}
IMAGE=${SCENARIO_KAFKA_IMAGE:-apache/kafka:3.9.1}
HEAP=${SCENARIO_KAFKA_HEAP:-1g}

nodes() {
	case "$1" in
	1) echo 1 ;;
	3) echo 2 3 4 ;;
	all) echo 1 2 3 4 ;;
	*)
		echo "cluster size must be 1 or 3" >&2
		exit 2
		;;
	esac
}

broker_port() { if [ "$1" -eq 1 ]; then echo "$BASE"; else echo $((BASE + $1 - 1)); fi; }
controller_port() { echo $(($(broker_port "$1") + 50)); }

bootstrap() {
	local out=()
	for n in $(nodes "$1"); do out+=("localhost:$(broker_port "$n")"); done
	(
		IFS=,
		echo "${out[*]}"
	)
}

stop() {
	for n in $(nodes "$1"); do docker rm -f "$PREFIX-$n" >/dev/null 2>&1 || true; done
}

start() {
	local size=$1 voters=() cluster_id
	stop "$size"
	for n in $(nodes "$size"); do voters+=("$n@localhost:$(controller_port "$n")"); done
	# A new cluster id per start, so preload manifests recorded against an old cluster are not reused.
	cluster_id=$(python3 -c 'import base64,uuid;print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("="))')
	for n in $(nodes "$size"); do
		docker run -d --name "$PREFIX-$n" --network host \
			-e KAFKA_NODE_ID="$n" -e KAFKA_PROCESS_ROLES=broker,controller \
			-e KAFKA_LISTENERS="PLAINTEXT://localhost:$(broker_port "$n"),CONTROLLER://localhost:$(controller_port "$n")" \
			-e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://localhost:$(broker_port "$n")" \
			-e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
			-e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
			-e KAFKA_CONTROLLER_QUORUM_VOTERS="$(
				IFS=,
				echo "${voters[*]}"
			)" \
			-e CLUSTER_ID="$cluster_id" \
			-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
			-e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
			-e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
			-e KAFKA_LOG_RETENTION_HOURS=-1 -e KAFKA_LOG_RETENTION_BYTES=-1 \
			-e KAFKA_HEAP_OPTS="-Xmx$HEAP -Xms$HEAP" \
			"$IMAGE" >/dev/null
	done
	local ready=0
	for _ in $(seq 120); do
		ready=0
		for n in $(nodes "$size"); do
			if docker logs "$PREFIX-$n" 2>&1 | grep -q "Kafka Server started"; then ready=$((ready + 1)); fi
		done
		[ "$ready" -eq "$size" ] && break
		sleep 1
	done
	if [ "$ready" -ne "$size" ]; then
		echo "only $ready/$size brokers started" >&2
		exit 1
	fi
	bootstrap "$size"
}

case "${1:-}" in
start) start "${2:?size}" ;;
stop) stop "${2:-all}" ;;
bootstrap) bootstrap "${2:?size}" ;;
status) docker ps -a --filter "name=^$PREFIX-" --format '{{.Names}}\t{{.Status}}' ;;
*)
	sed -n '2,13p' "$0"
	exit 2
	;;
esac
