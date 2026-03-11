# Getting Started

The goal of this project is to be testing common workloads (time series, key value) against a Cassandra cluster in 15 minutes or less.

## Installation

### Installing a Package

> **Note**: Installing from packages has not been migrated since I (Jon Haddad) began maintaining my own fork. I'll be updating this section soon.

## Building / Using the Stress Tool from Source

This is currently the only way to use the latest version of cassandra-easy-stress. I'm working on getting the packages updated.

First you'll need to clone and build the repo. You can grab the source here and build via the included gradle script:

```bash
$ git clone https://github.com/apache/cassandra-easy-stress.git
$ cd cassandra-easy-stress
$ ./gradlew shadowJar
```

You can now run the stress tool via the `bin/cassandra-easy-stress` script. This is not the same script you'll be running if you've installed from a package or the tarball.

You can also create a zip, tar, or deb package by doing the following:

```bash
$ ./gradlew distZip
$ ./gradlew distTar
$ ./gradlew deb
```

## Run Your First Stress Workload

Assuming you have either a CCM cluster or are running a single node
locally, you can run this quickstart.

Either add the `bin` directory to your PATH or from within cassandra-easy-stress
run the following command to execute 10,000 queries:

```
{{#include examples/cassandra-easy-stress-keyvalue.txt:1}}
```

You'll see the output of the keyspaces and tables that are created as well as some statistical information regarding the workload:

```bash
{{#include examples/cassandra-easy-stress-keyvalue.txt:2:}}
```

If you've made it this far, congrats! You've run your first workload.
