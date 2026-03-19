# Plan: Spark DataFrame Library for cassandra-easy-stress

## Goal

Reuse cassandra-easy-stress's data generation engine (workloads, field generators, partition key generators) to produce Spark DataFrames — completely decoupled from Cassandra. This enables:
- Generating realistic test data as DataFrames for Spark-based analytics pipelines
- Writing generated data to Parquet, CSV, Cassandra via Spark connector, etc.
- Leveraging the existing 17 workload definitions and 6 generator functions without rewriting them

## Architecture

### Current Coupling Problem

The data generation path is currently coupled to Cassandra in two places:

1. **`IStressWorkload.prepare(session: CqlSession)`** — prepares CQL statements against a live cluster
2. **`IStressRunner.getNextMutation() → Operation.Mutation(BoundStatement)`** — returns Cassandra driver `BoundStatement` objects

Everything *below* those (FieldGenerator, Registry, PartitionKeyGenerator) is already Cassandra-free.

### Proposed Design

```
┌─────────────────────────────────────────────────────────┐
│  New module: cassandra-easy-stress-spark                │
│                                                         │
│  SparkRowGenerator                                      │
│    - Takes a workload name + field overrides             │
│    - Parses the workload's schema() DDL → StructType    │
│    - Sets up Registry with workload's field generators   │
│    - Generates Row objects using generators + PK gen     │
│                                                         │
│  SparkDataFrameBuilder                                  │
│    - High-level API: workload → DataFrame               │
│    - Configurable: numRows, partitions, PK strategy     │
│    - Returns Dataset<Row> ready for any Spark sink       │
│                                                         │
│  CqlSchemaParser                                        │
│    - Parses CREATE TABLE DDL → Spark StructType          │
│    - Maps CQL types → Spark SQL types                    │
│                                                         │
└─────────────────────────────────────────────────────────┘
         │ depends on
         ▼
┌─────────────────────────────────────────────────────────┐
│  Existing: cassandra-easy-stress (core)                 │
│                                                         │
│  FieldGenerator interface (getText, getInt, getFloat)   │
│  Registry (field → generator mapping)                   │
│  FunctionLoader (@Function discovery)                   │
│  PartitionKeyGenerator (random, sequence, gaussian)     │
│  IStressWorkload.schema() + getFieldGenerators()        │
│  All generator functions (Random, Book, Names, etc.)    │
└─────────────────────────────────────────────────────────┘
```

## Implementation Steps

### Phase 1: CQL Schema → Spark StructType Parser

Create a `CqlSchemaParser` that extracts column names and types from the DDL strings returned by `IStressWorkload.schema()`.

**CQL → Spark type mapping:**
| CQL Type | Spark Type |
|----------|-----------|
| `text`, `varchar`, `ascii` | `StringType` |
| `int` | `IntegerType` |
| `bigint`, `counter` | `LongType` |
| `float` | `FloatType` |
| `double` | `DoubleType` |
| `boolean` | `BooleanType` |
| `timeuuid`, `uuid` | `StringType` (UUID as string) |
| `timestamp` | `TimestampType` |
| `blob` | `BinaryType` |
| `set<T>` | `ArrayType(T)` |
| `map<K,V>` | `MapType(K,V)` |
| `frozen<udt>` | `StructType` (flattened or nested) |

The parser needs to handle:
- `CREATE TABLE` and `CREATE TYPE` statements
- `PRIMARY KEY(...)` syntax (both inline and trailing)
- `WITH` clauses (strip them)
- Column names and types extraction

### Phase 2: SparkRowGenerator

A class that wires together the existing generators to produce `org.apache.spark.sql.Row` objects:

```kotlin
class SparkRowGenerator(
    val workloadName: String,
    val partitionCount: Long = 100_000,
    val partitionKeyStrategy: String = "random",  // random|sequence|normal
    val fieldOverrides: Map<String, String> = mapOf()  // "table.field" → "random(50,100)"
) {
    // Discovers workload via reflection (existing Workload.getWorkloads())
    // Parses schema() → StructType
    // Sets up Registry from getFieldGenerators() + overrides
    // Creates PartitionKeyGenerator

    fun generateRows(count: Long): Iterator<Row>
    fun sparkSchema(): StructType
}
```

Key design decisions:
- **Skips `prepare()`** entirely — no CqlSession needed
- **Skips `getRunner()`** — doesn't need BoundStatements
- **Reads schema DDL** for column names/types
- **Uses Registry** for field value generation
- **Uses PartitionKeyGenerator** for partition key values
- For clustering columns like `timeuuid`, generates values directly (e.g., `Uuids.timeBased().toString()`)

### Phase 3: SparkDataFrameBuilder (High-Level API)

```kotlin
// Usage from Spark (Scala/Kotlin):
val df = SparkDataFrameBuilder(spark)
    .workload("BasicTimeSeries")
    .rows(1_000_000)
    .partitions(10_000)
    .fieldOverride("sensor_data.data", "book(20,50)")
    .build()

df.write.parquet("/tmp/sensor_data")
// or
df.write.format("org.apache.spark.sql.cassandra")
    .options(mapOf("table" to "sensor_data", "keyspace" to "test"))
    .save()
```

Implementation:
- Uses `SparkRowGenerator` to produce an RDD of Rows
- Applies the parsed StructType as schema
- Returns a proper DataFrame
- Supports Spark parallelism by creating independent generators per partition

### Phase 4: Spark DataSource V2 (Optional, stretch goal)

Register as a proper Spark DataSource so users can:
```sql
CREATE TEMPORARY VIEW sensor_data
USING org.apache.cassandra.easystress.spark
OPTIONS (workload 'BasicTimeSeries', rows '1000000');

SELECT * FROM sensor_data;
```

## Module Structure

```
cassandra-easy-stress-spark/
├── build.gradle          # depends on :core, spark-sql
└── src/main/kotlin/org/apache/cassandra/easystress/spark/
    ├── CqlSchemaParser.kt        # DDL → StructType
    ├── SparkRowGenerator.kt      # Workload → Row iterator
    ├── SparkDataFrameBuilder.kt  # High-level builder API
    └── TypeMapping.kt            # CQL ↔ Spark type conversions
```

**Dependencies to add:**
- `org.apache.spark:spark-sql_2.12:3.5.x` (provided scope — user supplies Spark)
- The core cassandra-easy-stress module (for generators, workloads, registry)

## Refactoring Needed in Core

Minimal changes to the existing code:

1. **Extract a `core` module or ensure the shadow JAR is usable as a dependency**
   - Currently it's a single module with `application` plugin
   - Option A: Multi-module Gradle build with `:core` and `:cli` modules
   - Option B: Just depend on the project as-is, since the generators/workloads are already clean

2. **Make workload discovery work without CqlSession**
   - `Workload.getWorkloads()` already works without a session — it just discovers classes
   - `schema()` and `getFieldGenerators()` don't need a session
   - Only `prepare()` and `getRunner()` need a session — we simply skip them

3. **No changes to FieldGenerator, Registry, FunctionLoader, or any generators** — they're already Cassandra-free

## CQL Types That Need Special Handling

| Workload | Special Column | How to Generate |
|----------|---------------|----------------|
| BasicTimeSeries | `timeuuid` | `Uuids.timeBased().toString()` |
| UdtTimeSeries | `frozen<udt>` | Flatten to struct fields, generate each |
| CountersWide | `counter` | Generate as Long (counters are just longs in data) |
| Sets | `set<text>` | Generate N random text values as Array |
| Maps | `map<text,text>` | Generate N random key-value pairs as Map |
| SAI | Indexes | Indexes are DDL-only, ignore for data gen |

## Key Risks & Mitigations

1. **CQL DDL parsing is non-trivial** — Mitigate by using regex patterns for the subset of DDL this project actually generates (it's all simple CREATE TABLE statements, not arbitrary CQL)

2. **Spark version compatibility** — Use `provided` scope and target Spark 3.x API which is stable

3. **Thread safety of generators** — FieldGenerator implementations use `ThreadLocalRandom`, already safe. Spark partitions run in separate tasks, so each gets its own generator instance.

4. **Large-scale generation** — Spark parallelism handles this naturally. Each Spark partition gets its own `SparkRowGenerator` instance with an independent `PartitionKeyGenerator`.

## Open Questions

1. **Single module or multi-module?** Multi-module (`:core`, `:cli`, `:spark`) is cleaner but requires build restructuring. Starting with a single new module that depends on the whole project is simpler.

2. **Scala vs Kotlin for the Spark module?** Kotlin works fine with Spark, but Scala would feel more natural for Spark users. Recommend Kotlin for consistency with the rest of the project.

3. **Spark version?** Target Spark 3.5.x (latest stable). Use `provided` scope so users bring their own Spark.
