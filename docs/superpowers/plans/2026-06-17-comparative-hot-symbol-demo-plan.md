# Comparative Hot-Symbol Demo Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Add a runnable Docker Compose demo that compares Channels + symbol partitioning, Channels + dedup-key partitioning, and a correct TPL Dataflow approach on the same real PostgreSQL-backed workload.

**Architecture:** Keep the existing pipeline intact by adding a partitioner abstraction with the current symbol partitioner as default. Add a separate Dataflow pipeline that uses the same dedup and sink contracts. Add a demo host that feeds identical generated ticks into all three modes and streams metrics to a React UI.

**Tech Stack:** .NET 10, System.Threading.Channels, TPL Dataflow, Npgsql/PostgreSQL, React + Vite + TypeScript, Docker Compose.

---

### Task 1: Add Partitioning Abstraction

**Objective:** Make shard routing configurable without changing the default behavior.

**Files:**
- Create: `src/SeniorTicker.Processing/IShardPartitioner.cs`
- Create: `src/SeniorTicker.Processing/SymbolShardPartitioner.cs`
- Create: `src/SeniorTicker.Processing/DedupKeyShardPartitioner.cs`
- Modify: `src/SeniorTicker.Processing/TickPipeline.cs`
- Test: `tests/SeniorTicker.Processing.Tests/ShardPartitionerTests.cs`

**Verification:** Existing tests pass; new tests prove symbol hot keys stay on one shard and dedup keys spread one hot symbol.

### Task 2: Add Correct Dataflow Pipeline

**Objective:** Provide a TPL Dataflow implementation that keeps dedup state single-owner per partition.

**Files:**
- Modify: `Directory.Packages.props`
- Modify: `src/SeniorTicker.Processing/SeniorTicker.Processing.csproj`
- Create: `src/SeniorTicker.Processing/DataflowTickPipeline.cs`
- Test: `tests/SeniorTicker.Processing.Tests/DataflowTickPipelineTests.cs`

**Verification:** Dataflow pipeline deduplicates duplicates, drains on completion, and spreads hot symbols with dedup-key partitioning.

### Task 3: Add Demo Runtime

**Objective:** Run all three modes on the same generated stream and write to real PostgreSQL.

**Files:**
- Create: `src/SeniorTicker.DemoHost/SeniorTicker.DemoHost.csproj`
- Create: `src/SeniorTicker.DemoHost/Program.cs`
- Create demo services under `src/SeniorTicker.DemoHost/Demo/`
- Test: `tests/SeniorTicker.DemoHost.Tests/`

**Verification:** Testcontainers verifies table creation, writes, reset, and identical stream fan-out.

### Task 4: Add React UI

**Objective:** Show live side-by-side pipelines, controls, per-shard heat, queue depths, and Postgres row counts.

**Files:**
- Create: `frontend/senior-ticker-demo/package.json`
- Create: `frontend/senior-ticker-demo/src/*`
- Create: `frontend/senior-ticker-demo/Dockerfile`

**Verification:** `npm run build` succeeds and UI renders live SSE snapshots.

### Task 5: Add Docker Compose and Docs

**Objective:** Start PostgreSQL, demo API, and React UI with one command.

**Files:**
- Create: `docker-compose.yml`
- Create: `src/SeniorTicker.DemoHost/Dockerfile`
- Modify: `README.md`

**Verification:** `docker compose up --build` starts the stack; UI is available; API health returns OK.

### Task 6: Full Verification

**Objective:** Prove all modes and tests work.

**Commands:**

```bash
dotnet test SeniorTicker.sln
npm --prefix frontend/senior-ticker-demo run build
docker compose up --build
```

**Verification:** Tests pass; Docker services become healthy; UI shows three modes and live metrics.
