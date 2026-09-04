# go-etl-pipeline

Streaming CSV ingestion pipeline in Go. Processes large CSV files into PostgreSQL 
using bounded channels, a concurrent worker pool, and pgx batching — without 
loading the entire file into memory.

## Quick Start

```bash
git clone https://github.com/hamouksha/go-etl-pipeline.git
cd go-etl-pipeline

# Start PostgreSQL for integration tests
docker run -d --name etl-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=etl_test \
  -p 5432:5432 postgres:15

# Run all tests (unit + integration)
go test ./...

# Run the pipeline
go run ./cmd/pipeline --config=config.yaml
```

## Architecture

```mermaid 
flowchart TD

  A["input CSV File"] --> B["Streaming Parser (backpressure buffer)"]

  B["Streaming Parser (row {header + columns} validation)"] -->  C["Bounded Channel (backpressure buffer)"]

  C["Bounded Channel (backpressure buffer)"] --> D["Transformation Worker Pool (configurable goroutines)"]

  D["Transformation Worker Pool (configurable goroutines)"] --> E["Bounded Channel (backpressure buffer)"]

  E["Bounded Channel (backpressure buffer)"] --> F["pgx Batch Insert (PostgreSQL)"]
```

## Features

- **Streaming parser** — Row-by-row CSV reading via `encoding/csv`. Never loads the full file into memory.
- **Row validation** — Schema enforcement and type validation before ingestion. Invalid rows are logged and skipped.
- **Bounded channel backpressure** — Parser blocks when workers are saturated, preventing unbounded memory growth.
- **Concurrent worker pool** — Configurable goroutines, each with a dedicated pgx connection.
- **pgx batching** — Accumulates rows into `pgx.Batch` and flushes in a single round trip.
- **YAML configuration** — Pipeline behavior defined in a single config file.
- **Graceful shutdown** — Drains in-flight batches on SIGTERM/SIGINT.
- **Integration tested** — Full pipeline tests against a live PostgreSQL instance.

## Configuration

Create a `config.yaml` file:

Here a sample yaml config

```yaml

pipeline_name: "bankdataset_loader"
version: 1.0

source:
  source_name: "sample_test.csv"
  format: "csv"
  location: "data/bankdataset.csv"
  delimeter: ","

fields:
  - name: "Date"
    type: "timestamp"
    layout: "2006-01-02"
    required: true
  - name: "Domain"
    type: "string"
    required: true
  - name: "Location"
    type: "string"
    required: true
  - name: "Value"
    type: "float64"
    min: 0.0
    required: true
  - name: "Transaction_count"
    type: "int"
    min: 0
    required: true

target:
  db: "postgres://postgres:postgres@localhost:5432/ingot"
  table: "bankdata"
```

### Config Fields

| Field | Default | Description |
|-------|---------|-------------|
| `pipeline.input.path` | `""` | Path to input CSV file |
| `pipeline.input.format` | `"csv"` | Input format (`csv` only; `json` planned) |
| `pipeline.database.dsn` | `""` | PostgreSQL connection string |
| `pipeline.database.batch_size` | `100` | Rows per pgx batch |
| `pipeline.workers.count` | `runtime.NumCPU()` | Number of concurrent workers |
| `pipeline.buffer.size` | `workers * batch_size` | Channel buffer size |

## Design Decisions

### Why channels over a shared slice?
A shared slice would require a mutex on every append and keep all parsed rows in 
heap memory. Channels decouple the parser from workers and provide natural 
backpressure: when the buffer fills, the parser blocks until a worker is ready.

### Why pgx over database/sql?
`pgx` supports native batching via `pgx.Batch`, which pipelines multiple INSERTs 
in one round trip. `database/sql` would require manual transaction wrapping or 
`COPY FROM`, which is less flexible for per-row validation and error tracking.

### Worker pool sizing
Each worker holds one database connection. The pool size defaults to 
`runtime.NumCPU()` but is configurable via `workers.count`. This prevents Postgres 
connection exhaustion while maximizing throughput.

### Row validation before batching
Validation happens before rows enter the batch. This isolates bad data: one 
malformed row is logged and skipped rather than failing an entire batch insert.

### Why YAML configuration?
A config file keeps runtime parameters version-controlled and reviewable. It 
also separates operational concerns (DSN, batch size) from code, making the 
pipeline reusable across environments without recompilation.

## Testing

```bash
# Unit tests only (no database required)
go test ./... -short

# All tests including integration (requires Postgres)
go test ./...

# Run with verbose output
go test ./... -v
```

Integration tests spin up a temporary table, run the full pipeline, and assert 
row counts and data integrity.

## Benchmarks

> Planned: throughput and memory comparison vs. naive all-in-memory approach.

## Tech Stack

- Go 1.22+
- PostgreSQL 14+
- [pgx/v5](https://github.com/jackc/pgx) — PostgreSQL driver with batch support
- `encoding/csv` — standard library streaming parser
- `gopkg.in/yaml.v3` — YAML configuration parsing

## What I Learned

- **Channel buffer tuning is critical:** Unbuffered channels create lock-step 
  coupling between parser and workers. Overly buffered channels hide backpressure 
  and spike memory. The buffer size should relate to batch size and worker count.
  
- **Batch error isolation:** `pgx.Batch` sends all queries in one round trip, 
  but a single bad row can fail the entire batch. Pre-validating rows before 
  batching was the key design decision for clean error handling.
  
- **Integration testing with real databases:** Mocking `pgx` would hide real 
  behavior around connection pooling, transaction boundaries, and batch execution. 
  Testing against a live Postgres container caught race conditions that unit 
  tests missed.

- **Config-driven pipelines:** Using YAML instead of flags makes the pipeline 
  easier to run in CI/CD and share across team members without shell scripts.

## Roadmap

- [ ] JSON input support
- [ ] Benchmarks vs. naive in-memory approach
- [ ] Dead letter queue for invalid rows
- [ ] Resume from failure (track offset in WAL)

## License

MIT

