# cassandra-easy-stress

cassandra-easy-stress is a workload-centric stress tool for Apache Cassandra, written in Kotlin.
Workloads are easy to write and because they are written in code, you
have the ultimate flexibility to build whatever you want without having to learn and
operate around the restrictions of a configuration DSL. Workloads can be tweaked via command line
parameters to make them fit your environment more closely.

One of the goals of cassandra-easy-stress is to provide enough pre-designed
workloads *out of the box*, so it's unnecessary to code up a workload for
most use cases. For instance, it's very common to have a key value
workload, and want to test that. cassandra-easy-stress allows you to customize a
pre-configured key-value workload, using simple parameters to modify the
workload to fit your needs. Several workloads are included, such as:

* Time Series
* Key / Value
* Materialized Views
* Collections (maps)
* Counters

The tool is flexible enough to design workloads which leverage multiple
(thousands) of tables, hitting them as needed. Statistics are
automatically captured by the Dropwizard metrics library.
