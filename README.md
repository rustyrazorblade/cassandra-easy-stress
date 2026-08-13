# cassandra-easy-stress

## A workload centric stress tool and framework designed for ease of use.

[![CI](https://github.com/apache/cassandra-easy-stress/actions/workflows/ci.yml/badge.svg)](https://github.com/apache/cassandra-easy-stress/actions/workflows/ci.yml)

cassandra-easy-stress is a powerful and flexible tool for performing benchmarks and testing data models for Apache Cassandra.

Most benchmarking tools require learning complex configuration systems before you can run your first test. cassandra-easy-stress provides pre-built workloads for common Cassandra patterns. Modify these workloads with flexible parameters to match your environment, or write custom workloads in Kotlin when you need full control.

Full docs are here: https://apache.github.io/cassandra-easy-stress/

# Installation

The easiest way to get started on Linux is to use system packages.
Instructions for installation can be found here: https://apache.github.io/cassandra-easy-stress/#_installation


# Building

Clone this repo, then build with gradle:

    git clone https://github.com/apache/cassandra-easy-stress.git
    cd cassandra-easy-stress
    ./gradlew shadowJar

Use the shell script wrapper to start and get help:

    bin/cassandra-easy-stress -h

# Examples

Time series workload with a billion operations:

    bin/cassandra-easy-stress run BasicTimeSeries -i 1B

Key value workload with a million operations across 5k partitions, 50:50 read:write ratio:

    bin/cassandra-easy-stress run KeyValue -i 1M -p 5k -r .5


Time series workload, using TWCS:

    bin/cassandra-easy-stress run BasicTimeSeries -i 10M --compaction "{'class':'TimeWindowCompactionStrategy', 'compaction_window_size': 1, 'compaction_window_unit': 'DAYS'}"

Time series workload with a run lasting 1h and 30mins:

    bin/cassandra-easy-stress run BasicTimeSeries -d "1h30m"

Time series workload with Cassandra Authentication enabled:

    bin/cassandra-easy-stress run BasicTimeSeries -d '30m' -U '<username>' -P '<password>'
    **Note**: The quotes are mandatory around the username/password
    if they contain special chararacters, which is pretty common for password

# Capturing Client Latencies to Apache Parquet

You can capture detailed client latency metrics to an Apache Parquet file using the `--parquet` flag:

```
bin/cassandra-easy-stress run KeyValue --duration 5m --parquet rawlog.parquet
```

This writes every operation's latency data (including request time, service time, success/failure status, and error details) to a Parquet file for post-run analysis with tools like DuckDB, Pandas, or Spark. See the [full documentation](https://apache.github.io/cassandra-easy-stress/#_capturing_client_latencies_to_apache_parquet) for schema details and example queries.

# Generating docs

Docs are served out of /docs and can be rebuild using `./gradlew docs`.

# Testing

## Quick Start

Run all tests against the default version (Cassandra 5.0):

```bash
./gradlew test
```

Run tests against a specific version:

```bash
./gradlew test40      # Cassandra 4.0
./gradlew test41      # Cassandra 4.1
./gradlew test50      # Cassandra 5.0
```

Run tests against all versions sequentially:

```bash
./gradlew testAllVersions
```

## Test Infrastructure

### Testcontainers

All integration tests use [Testcontainers](https://www.testcontainers.org/) to automatically manage Cassandra instances. This means:

- **No manual setup required**: Docker is the only prerequisite
- **Isolated test environments**: Each test class gets a fresh Cassandra container
- **Automatic cleanup**: Containers are stopped and removed after tests complete
- **Version flexibility**: Different Cassandra versions can be tested without installing anything

When a test class extends `CassandraTestBase`, the framework automatically:

1. Detects the `CASSANDRA_VERSION` environment variable (defaults to "5.0")
2. Builds a Docker image from the corresponding Dockerfile in `docker/cassandra-{version}/`
3. Starts a Cassandra container and waits for it to be ready
4. Establishes a CQL session with appropriate timeouts for testing
5. Cleans up and stops the container when tests finish

### Custom Docker Images

Each Cassandra version uses a custom Dockerfile that enables experimental features:

- **Materialized Views**: Enabled via `materialized_views_enabled: true`
- **Increased Timeouts**: Higher read/write/range timeouts for test stability
- **Memory Configuration**: Conservative heap settings suitable for containers

The Dockerfiles are located at:
- `docker/cassandra-4.0/Dockerfile`
- `docker/cassandra-4.1/Dockerfile`
- `docker/cassandra-5.0/Dockerfile`

## Workload Filtering Annotations

Some workloads require specific Cassandra versions or features. Annotations control when workloads are tested.

### @MinimumVersion

Marks workloads that require a minimum Cassandra version. Tests automatically skip these workloads on older versions.

**Example:**
```kotlin
@MinimumVersion("5.0")
class SAI : IStressWorkload {
    // SAI indexes are only available in Cassandra 5.0+
}
```

**How it works:**
- The `CASSANDRA_VERSION` environment variable determines which version is running
- `Workload.getWorkloadsForTesting()` filters out workloads where the version doesn't meet the minimum
- Version comparison supports point releases: "5.0", "5.1", etc.

**Currently annotated workloads:**
- `MaterializedViews` - Requires 5.0+ (materialized views enabled)
- `SAI` - Requires 5.0+ (Storage Attached Indexes)

### @RequireDSE

Marks workloads that require DataStax Enterprise features. These are skipped by default.

**Example:**
```kotlin
@RequireDSE
class DSESearch : IStressWorkload {
    // Uses DSE Search (Solr) functionality
}
```

**Enable in tests:**
```bash
TEST_DSE=1 ./gradlew test
```

**Currently annotated workloads:**
- `DSESearch` - Uses DSE Search (Solr)

### @RequireAccord

Marks workloads that use Accord transaction features (available in Cassandra 6.0+). These are skipped by default.

**Example:**
```kotlin
@RequireAccord
class TxnCounter : IStressWorkload {
    // Uses Accord transactions
}
```

**Enable in tests:**
```bash
TEST_ACCORD=1 ./gradlew test
```

**Currently annotated workloads:**
- `TxnCounter` - Uses Accord transactions

### Running All Special Workloads

To run all tests including DSE and Accord workloads:

```bash
TEST_DSE=1 TEST_ACCORD=1 ./gradlew test
```

## Gradle Test Tasks

### Version-Specific Tasks

Individual test tasks are created for each Cassandra version:

```bash
./gradlew test40      # Cassandra 4.0
./gradlew test41      # Cassandra 4.1
./gradlew test50      # Cassandra 5.0
```

These are proper Gradle `Test` tasks, so they support all standard Test task options:

```bash
# Run only specific tests
./gradlew test50 --tests "*KeyValue*"

# Enable debug mode
./gradlew test50 --debug-jvm

# Rerun even if up-to-date
./gradlew test40 --rerun-tasks
```

**How they work:**
- Each task sets `CASSANDRA_VERSION` environment variable to the corresponding version
- Uses the same test sources and classpath as the main `test` task
- Integrates with Gradle's task graph for proper caching and dependency management

### testAllVersions Task

Runs tests against all Cassandra versions sequentially:

```bash
./gradlew testAllVersions
```

This task:
- Depends on `test40`, `test41`, and `test50`
- Enforces sequential execution using `mustRunAfter` to prevent Docker resource conflicts
- Stops on the first failure and reports which version failed
- Displays a summary of all test results

**Execution time:**
- With cached Docker images: ~15-25 minutes total

### Standard test Task

The standard `test` task respects the `CASSANDRA_VERSION` environment variable:

```bash
# Test against Cassandra 4.1
CASSANDRA_VERSION=4.1 ./gradlew test

# Test against Cassandra 5.0 (default)
CASSANDRA_VERSION=5.0 ./gradlew test
```

If `CASSANDRA_VERSION` is not set, it defaults to "5.0".

## Continuous Integration

### CI Workflow Structure

The GitHub Actions CI workflow (`.github/workflows/ci.yml`) runs on every push and pull request.

**Test matrix:**
- **Java versions**: 21 and 25 (current + next LTS)
- **Cassandra versions**: 4.0, 4.1, 5.0

This creates 6 test jobs total:
- Cassandra 4.0 on Java 21 and 25
- Cassandra 4.1 on Java 21 and 25
- Cassandra 5.0 on Java 21 and 25

**Workflow steps:**

1. **Checkout and setup**: Check out code, set up JDK and Gradle
2. **Run tests**: Execute the version-specific test task (e.g., `./gradlew test50`)
3. **Code quality**: Run ktlint and detekt checks in parallel
4. **Coverage**: Generate code coverage reports with kover (using default test task only)
5. **Build**: Create distribution tarball artifact (main branch only, after all checks pass)
   - Artifact: `cassandra-easy-stress-{version}.tar.gz` 
   - Retention: 90 days
   - Contains: binaries, all dependencies, and LICENSE

### Running CI Tests Locally

Replicate CI behavior locally:

```bash
# Run the same tests as CI for Cassandra 5.0
./gradlew test50 ktlintCheck detekt

# Test all versions like CI does (sequentially)
./gradlew testAllVersions

# Generate coverage report
./gradlew test koverXmlReport
```

## Writing Tests

Integration tests extend `CassandraTestBase`:

```kotlin
class MyWorkloadTest : CassandraTestBase() {
    @Test
    fun testWorkload() {
        // connection is available from CassandraTestBase
        val result = connection.execute("SELECT * FROM system.local")
        assertThat(result).isNotNull
    }
}
```

**Available properties from CassandraTestBase:**
- `connection`: CqlSession instance
- `ip`: Container IP address
- `port`: Mapped CQL port
- `localDc`: Datacenter name (defaults to "datacenter1")

**Utility methods:**
- `cleanupKeyspace()`: Drops the test keyspace
- `keyspaceExists()`: Checks if test keyspace exists
- `getCassandraVersion()`: Returns Cassandra release version string

**Test guidelines:**
- Use `CassandraTestBase` for integration tests requiring Cassandra
- Use `@BeforeEach` to ensure clean state with `cleanupKeyspace()`
- Use AssertJ assertions for clear, fluent test assertions
- Test against multiple versions if workload behavior varies by version
- Don't use `runBlocking` in tests (use `kotlinx.coroutines.test.runTest` instead)

# MCP Server Integration

cassandra-easy-stress includes a Model Context Protocol (MCP) server that allows AI assistants to interact with the stress testing tool.  To start in server mode:

```
cassandra-easy-stress server
```

## Testing with Claude Code

To test the MCP server integration with Claude Code:

1. Add the MCP server (assuming it's running locally):
   ```bash
   claude mcp add -t sse cassandra-easy-stress http://localhost:9000/sse
   ```

2. Run the MCP integration test prompt:
   ```bash
   /test-mcp
   ```

This will verify that the MCP server is properly configured and accessible from Claude Code.

## Other MCP-Compatible Tools

The MCP server should work with any tool that supports the Model Context Protocol. If you've successfully integrated cassandra-easy-stress with another MCP-compatible tool, please send a PR with instructions and we'll update this README.
