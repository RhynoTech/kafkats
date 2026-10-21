# @kafkats/benchmark

Performance benchmarks comparing @kafkats/client with kafkajs.

## Commands

```bash
pnpm bench:producer   # Run producer benchmarks
pnpm bench:consumer   # Run consumer benchmarks
pnpm bench:share      # Run ShareConsumer benchmarks (kafkats only)
pnpm bench:all        # Run all benchmarks
pnpm scenarios        # Production-shaped scenarios against any client tree (see below)
```

## Structure

```
src/
├── producer-benchmark.ts  # Producer throughput comparisons
├── consumer-benchmark.ts  # Consumer throughput comparisons
├── kafka-cluster.ts       # Testcontainers Kafka setup
├── utils.ts               # Benchmark utilities
├── index.ts               # Entry point for all benchmarks
└── scenarios/             # Scenario harness: cli, matrix, report, runner, s1-s6
scripts/
└── scenario-cluster.sh    # 1- or 3-node KRaft cluster for the scenarios
```

## Running Benchmarks

Benchmarks use a persistent Docker Compose Kafka cluster. Start it once, then run benchmarks repeatedly:

```bash
KAFKA_ADVERTISED_HOST=localhost docker compose -f packages/benchmark/docker-compose.yml up -d
KAFKA_BROKERS=localhost:19292,localhost:19293,localhost:19294 pnpm -C packages/benchmark bench:all
#
# If running from a container/devcontainer, use:
# KAFKA_ADVERTISED_HOST=host.docker.internal docker compose -f packages/benchmark/docker-compose.yml up -d
# KAFKA_BROKERS=host.docker.internal:19292,host.docker.internal:19293,host.docker.internal:19294 pnpm -C packages/benchmark bench:all
```

### Share Groups

The benchmark cluster is configured for Kafka Share Groups (`share.version=1`), but Share Groups are also a **feature flag**. After the cluster is up, run this once:

```bash
docker exec kafkats-bench-kafka-1 /opt/kafka/bin/kafka-features.sh --bootstrap-server localhost:19092 upgrade --feature share.version=1
```

Share benchmarks only run `@kafkats/client` (no comparisons), since Share Groups are not widely supported in other clients.

### Flags

All `bench:*` scripts run the same runner (`src/index.ts`).

```bash
pnpm -C packages/benchmark bench:all -- --iterations 10 --warmup 1
pnpm -C packages/benchmark bench:producer -- --messageCount 50000 --messageSize 1024 --batchSize 500
pnpm -C packages/benchmark bench:consumer -- --messageCount 100000 --messageSize 1024
pnpm -C packages/benchmark bench:share -- --messageCount 100000 --messageSize 1024 --shareConsumers 2

# Optional diagnostics / trace + NDJSON output (paths are relative to packages/benchmark)
pnpm -C packages/benchmark bench:all -- --diagnostics --trace --json results/bench.ndjson
```

Results compare:

- **@kafkats/client**: This library
- **kafkajs**: Popular existing Kafka client

## Adding Benchmarks

When adding new benchmarks:

1. Use `kafka-cluster.ts` helpers for container setup
2. Use `utils.ts` for timing and statistics
3. Ensure fair comparison (same message sizes, batch settings, etc.)
4. Report messages/sec and MB/sec metrics

## Scenario benchmarks

`src/scenarios/` benchmarks and checks **any kafkats source tree** without changing it: the client is
imported from the path given with `--client` (another worktree's `packages/client/src/index.ts`), so the
same scenarios can be compared across branches. Only public client APIs are used.

```bash
# from the repository root; SCENARIO_PORT_BASE (default 39100) must match between the two commands
pnpm -C packages/benchmark scenarios:cluster start 1          # or 3; `stop all` removes the containers
pnpm -C packages/benchmark scenarios --client /abs/wt/packages/client/src/index.ts \
    --scenario all --brokers 1 --out /abs/results/wt.json     # --scenario s1,s6 or fairness,producer-split
pnpm -C packages/benchmark scenarios:matrix --brokers 1 --out-dir /abs/results wt-a wt-b   # JSON each + summary.md
pnpm -C packages/benchmark scenarios:report /abs/results/*.json --out summary.md
```

Options: `--repeat N` (headline values are medians), `--scale X` (multiplies every measurement window),
`--bootstrap host:port,...` (instead of `--brokers`), `--loader <client index.ts>`, `--state-dir`,
`--cpu-prof` (writes a `.cpuprof/` directory per run next to the logs). Worktrees need `pnpm install`.

| Scenario            | Shape                                                                                                                                                                                                                                                                                                                                                           | Headline                                                                                        |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `s1-fairness`       | 80 partitions x 50k ~530 B JSON records, `runBatch` concurrency 3, 20-70 ms sleep, pmb 512 KiB, 40 s                                                                                                                                                                                                                                                            | partitions never served, served by 10 s / 30 s, max gap, min/max per partition, Gini, rec/s     |
| `s2-tm-pod`         | Ticketmaster pod: 1 backlogged customers partition (schemaless Debezium, 3.2 KB decoded, ~1 KB wire in 7-record snappy batches) first in the subscription, 10 co-located partitions of 7 TM topics at 20 rec/s each; EOS handler 93 ms + 0.15 ms/record with `tx.send` keyed by userID over 80 partitions and `tx.sendOffsets`; range, pmb 1 MiB, 8 lanes, 40 s | customers rec/s, trickle delivered, p50/p99 latency, transaction overhead                       |
| `s3-rekey`          | fans CDC 3 topics x 3 partitions (300k fan_users updates backlogged) + 80-partition user-events at 2 rec/s per partition; EOS, 10 lanes, minBytes 1 MiB, maxWaitMs 5000, output keyed by workspace (3 hot) over 10 partitions, 45 s                                                                                                                             | fans rec/s, user-events delivered and p50, largest output bytes per partition per send          |
| `s4-decode-cpu`     | drain ~200 MiB wire of customers records over 40 partitions with a no-op handler, 3 passes                                                                                                                                                                                                                                                                      | wire / decoded MiB/s, main-thread CPU ms per wire MiB, event-loop delay, heartbeat lateness, GC |
| `s5-memory`         | same 40 partitions all backlogged, sleep handler, 20 s, memory sampled every 100 ms                                                                                                                                                                                                                                                                             | peak RSS / heapUsed / external over baseline, as multiples of the 10 x pmb buffer               |
| `s6-producer-split` | one transactional `tx.send` of 0.5/1.5/3/6 MiB to one partition, JSON (~3.6x) and random bytes, default topic and `max.message.bytes=100000`, then a read_committed check                                                                                                                                                                                       | per size `ok`/`MTL`/`fail` and produce requests, verify mismatches                              |

How a run works:

- Each scenario run is three child processes: **preload** (the loader client, by default this
  package's sibling `packages/client`, creates the datasets and this run's fresh topics), **run** (the
  client under test measures) and **cleanup** (deletes the run's topics). A child loads exactly one
  client tree, with `TSX_TSCONFIG_PATH` pointing at that tree's tsconfig so its `@/` imports resolve.
- Datasets are generated from seeded RNGs, written with exact RecordBatch layouts, and reused across
  runs and worktrees; a manifest per cluster id (in `--state-dir`, default `<out dir>/.scenario-state`)
  records their wire bytes. `scenario-cluster.sh start` makes a new cluster id, so manifests never
  outlive the data.
- Windows open on the first delivered batch, so group join is not measured.
- Produce/Fetch/Heartbeat requests and socket bytes are counted below the client by wrapping
  `net.Socket` (`wire-probe.ts`), so the counts do not depend on client instrumentation.
- Each run's logs, setup and raw result are kept in `<out>.logs/`.

To add a scenario, implement `Scenario` (`types.ts`) and register it in `registry.ts`; keep per-run
topics in `runTopics()` and reusable data in `datasets()` (bump the dataset id when its shape changes).
