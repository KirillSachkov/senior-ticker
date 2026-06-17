# Comparative Hot-Symbol Demo Design

## Goal

Show three correct processing approaches on the same hot-ticker workload:

1. Channels with `Symbol` partitioning.
2. Channels with dedup-key partitioning.
3. TPL Dataflow with dedup-key partitioning.

The demo must use real PostgreSQL through Docker Compose, expose a React UI, and keep the existing tested pipeline path intact.

## Architecture

Add a partitioning abstraction to `SeniorTicker.Processing`:

```text
IShardPartitioner
  SymbolShardPartitioner
  DedupKeyShardPartitioner
```

`TickPipeline` keeps the same bounded channel topology and accepts an `IShardPartitioner`. The default remains `SymbolShardPartitioner`.

Add `DataflowTickPipeline` as a separate implementation for the correct TPL Dataflow approach. It uses bounded blocks, explicit completion propagation, and one single-reader dedup owner per partition. It must not share mutable dedup state across parallel delegates.

Add a demo host:

```text
SeniorTicker.DemoHost
  ComparativeDemoRunner
  DemoTickGenerator
  DemoPostgresSink
  SSE metrics endpoint
```

The runner feeds the same generated tick stream into all enabled modes. Each mode writes to real PostgreSQL with its own `mode` value in a demo table so results remain comparable.

## UI

Create a React app under `frontend/senior-ticker-demo`.

Controls:

- start, stop, reset
- tick rate
- hot symbol ratio
- duplicate rate
- shard count
- writer count
- batch size

Live-safe controls (`rate`, `hotSymbolRatio`, `duplicateRate`) apply immediately. Topology controls (`shardCount`, `writerCount`, `batchSize`) apply via stop/drain/restart.

The UI shows three side-by-side pipelines with per-shard throughput, backlog, deduplicated count, written count, and PostgreSQL row count.

## Docker Compose

Compose runs:

- `postgres`
- `demo-api`
- `demo-ui`

The default path is:

```bash
docker compose up --build
```

The UI talks to the API inside the compose network and the browser through a proxied `/api` path.

## Tests

Processing tests:

- symbol partitioning routes one hot symbol to one shard;
- dedup-key partitioning spreads one hot symbol across shards;
- duplicate tick keys still route to one shard;
- both Channels modes deduplicate correctly;
- Dataflow mode deduplicates correctly;
- comparative runner feeds identical ticks to all modes.

Persistence/demo tests:

- demo sink creates its table;
- demo sink writes rows to PostgreSQL;
- reset truncates demo rows.

## Explicit Boundaries

The demo compares behavior shapes, not universal throughput numbers. Absolute throughput depends on the machine and Docker limits.

Changing shard count in a running in-memory topology restarts pipelines after drain. The UI must not pretend this can be changed without moving state.
