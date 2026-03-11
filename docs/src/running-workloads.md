# Running Workloads

You'll probably want to do a bit more than simply run a few thousand queries against a KeyValue table with default settings. The nice part about cassandra-easy-stress is that it not only comes with a variety of workloads that you can run to test your cluster, but that it allows you to change many of the parameters. In the quickstart example we used the `-n` flag to change the total number of operations `cassandra-easy-stress` will execute against the database. There are many more options available, this section will cover some of them.

## General Help

`cassandra-easy-stress` will display the help if the `cassandra-easy-stress` command is run without any arguments or if the `--help` flag is passed:

```bash
{{#include examples/cassandra-easy-stress-help.txt}}
```

## Listing All Workloads

```
{{#include examples/list-all.txt}}
```

## Getting Information About a Workload

It's possible to get (some) information about a workload by using the info command. This area is a bit lacking at the moment. It currently only provides the schema and default read rate.

```
{{#include examples/info-key-value.txt}}
```

## Human Friendly Numbers

Whenever possible we try to use human friendly numbers. Typing out `-n 1000000000` is error prone and hard to read, `-n 1B` is much easier.

| Suffix | Implication | Example | Equivalent |
|--------|-------------|---------|------------|
| k | Thousand | 1k | 1,000 |
| m | Million | 1m | 1,000,000 |
| b | Billion | 1b | 1,000,000,000 |
