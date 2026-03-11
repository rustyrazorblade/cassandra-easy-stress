# Observability

## Logging

`cassandra-easy-stress` uses the [Log4J 2](https://logging.apache.org/) logging framework.

You can find the default log4j config in [conf](https://github.com/apache/cassandra-easy-stress/blob/main/src/main/resources/log4j2.yaml). This should be suitable for most use cases.

To use your own logging configuration, simply set the shell variable `CASSANDRA_EASY_STRESS_LOG4J` to the path of the new logging configuration before running `cassandra-easy-stress` to point to the config file of your choice.

For more information on how to configure Log4J 2 please see the [configuration documentation](https://logging.apache.org/log4j/2.x/manual/configuration.html).

### Debian Package

The Debian package installs a basic configuration file to `/etc/cassandra-easy-stress/log4j2.yaml`.

## Exporting Metrics

cassandra-easy-stress automatically runs an HTTP server exporting metrics in Prometheus format on port 9501.

## Capturing Client Latencies to Apache Parquet

You can capture detailed client latency metrics to an Apache Parquet file using the `--parquet` flag followed by a path to a file or directory:

```
$ bin/cassandra-easy-stress run KeyValue --duration 5m --parquet rawlog.parquet
```

This writes operation metrics including operation type, success/failure status, start time, and duration to a Parquet file that can be analyzed later with data analysis tools like Pandas, Spark, DuckDB, or visualization tools that support the Parquet format.

If a directory is provided instead of a file, cassandra-easy-stress will automatically create an appropriately named file in that directory.

### Parquet Schema Reference

Each row in the Parquet file represents a single operation with the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `operation` | `binary (UTF8)` | Operation type (e.g. `Insert`, `Select`) — derived from the statement class name |
| `success` | `boolean` | Whether the operation completed successfully |
| `failure_reason` | `binary (UTF8)` | Exception class name if the operation failed (empty string on success) |
| `failure_stacktrace` | `binary (UTF8)` | Full stacktrace if the operation failed (empty string on success) |
| `request_start_time_ms` | `int64` | Epoch milliseconds when the client intended to send the request |
| `request_duration_ns` | `int64` | Total elapsed time in nanoseconds from request creation to response (includes queue time) |
| `service_start_time_ms` | `int64` | Epoch milliseconds when the request was actually sent to the database |
| `service_duration_ns` | `int64` | Actual database processing time in nanoseconds (excludes queue time) |

> **Note**: Every operation is captured (not sampled), so file sizes grow proportionally with the number of operations.

### Analyzing Parquet Files with DuckDB

The Parquet files created by cassandra-easy-stress can be easily analyzed using DuckDB, a lightweight analytical database engine. Here are some example queries to get you started:

```sql
-- Show summary statistics for operation latencies for every minute
SELECT date_trunc('minute', epoch_ms(request_start_time_ms)) as minute,
       COUNT(*) as count,
       AVG(request_duration_ns / 1000 / 1000) as avg,
       MIN(request_duration_ns / 1000 / 1000) as min,
       MAX(request_duration_ns / 1000 / 1000) as max,
       APPROX_QUANTILE(request_duration_ns / 1000 / 1000, .5) as p50,
       APPROX_QUANTILE(request_duration_ns / 1000 / 1000, .9) as p90,
       APPROX_QUANTILE(request_duration_ns / 1000 / 1000, .99) as p99,
       COUNT(CASE WHEN failure_reason != '' THEN 1 END) AS errors,
       COUNT(CASE WHEN failure_reason = 'ReadTimeoutException' THEN 1 END) AS read_timeouts,
       COUNT(CASE WHEN failure_reason = 'WriteTimeoutException' THEN 1 END) AS write_timeouts,
FROM read_parquet('rawlog.parquet')
GROUP BY minute
ORDER BY minute;
```

Example output:

```
┌─────────────────────┬────────┬─────────────────────┬──────────┬────────────────────┬─────────────────────┬─────────────────────┬────────────────────┬────────┬───────────────┬────────────────┐
│       minute        │ count  │         avg         │   min    │        max         │         p50         │         p90         │        p99         │ errors │ read_timeouts │ write_timeouts │
│      timestamp      │ int64  │       double        │  double  │       double       │       double        │       double        │       double       │ int64  │     int64     │     int64      │
├─────────────────────┼────────┼─────────────────────┼──────────┼────────────────────┼─────────────────────┼─────────────────────┼────────────────────┼────────┼───────────────┼────────────────┤
│ 2025-05-23 22:45:00 │ 141911 │  18.891404855317813 │ 0.088042 │        1305.993875 │ 0.23617621307864622 │  0.5454138286498701 │   755.160273634975 │      0 │             0 │              0 │
│ 2025-05-23 22:46:00 │ 300081 │ 0.26154326542833034 │ 0.091458 │          16.620042 │  0.2198726495794396 │ 0.28866495065114356 │ 1.0759477146627234 │      0 │             0 │              0 │
│ 2025-05-23 22:47:00 │ 300075 │ 0.29655502679997087 │ 0.089208 │          19.247875 │  0.2241928371244093 │  0.3096208807374364 │ 1.8582042492087465 │      0 │             0 │              0 │
│ 2025-05-23 22:48:00 │ 298543 │  0.6524298801211285 │ 0.093666 │ 198.99904199999997 │ 0.22677466153454573 │ 0.33573102120265713 │  9.839418314581492 │      0 │             0 │              0 │
│ 2025-05-23 22:49:00 │ 300053 │ 0.30696147925533085 │ 0.100167 │          64.216666 │  0.2249157848195121 │  0.3072763658664282 │ 1.6342296967730887 │      0 │             0 │              0 │
│ 2025-05-23 22:50:00 │  24765 │  0.4530204902079576 │  0.12675 │          39.548167 │  0.2259263537252715 │ 0.30608390597020046 │ 7.7139616210838575 │      0 │             0 │              0 │
└─────────────────────┴────────┴─────────────────────┴──────────┴────────────────────┴─────────────────────┴─────────────────────┴────────────────────┴────────┴───────────────┴────────────────┘
```

```sql
-- Show error counts
SELECT failure_reason, count(*)
FROM read_parquet('rawlog.parquet')
WHERE failure_reason != ''
GROUP BY failure_reason;
```

You can also use DuckDB through its various clients including Python, R, Java, and JDBC.

### Understanding Request Time vs Service Time

When analyzing latency data from the Parquet files, it's important to understand the distinction between two key metrics:

* **Service Time**: This is the actual time it takes for the database to process a request and return a response once the request is received by the database. It measures only the execution time of the operation.

* **Request Time**: This is the total time from when the client intended to make the request until receiving the response. It includes the service time plus any queue time or delays that might have occurred before the request was actually sent to the database.

The difference between these metrics is critical for understanding coordinated omission, a common problem in performance testing where the test client doesn't accurately capture the full latency that would be experienced by real users when the system is under load.

For example, if your database is overloaded and can only process 100 operations per second, but your test is trying to send 200 operations per second:

* A naïve benchmark would only measure the service time of the operations that actually got processed, missing the fact that half the operations were delayed.
* A properly instrumented benchmark (like cassandra-easy-stress) captures the request time, which includes how long operations had to wait in a queue.

When using the Parquet files for analysis, you can examine both metrics to get a more complete picture of your system's performance under load:

```sql
-- Compare average service time vs request time by operation type
SELECT
    operation,
    AVG(service_duration_ns / 1000 / 1000) as avg_service_time_ms,
    AVG(request_duration_ns / 1000 / 1000) as avg_request_time_ms,
    AVG(request_duration_ns - service_duration_ns) / 1000 / 1000 as avg_queue_time_ms
FROM read_parquet('rawlog.parquet')
GROUP BY operation;
```

A significant difference between average request time and average service time indicates queuing or scheduling delays in your system, which can be an early warning sign of performance bottlenecks.
