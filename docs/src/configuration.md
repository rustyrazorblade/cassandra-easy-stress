# Configuration

## Running a Stress Test for a Given Duration

You might need to run a stress test for a given duration instead of providing a number of operations, especially in case of multithreaded stress runs. This is done by providing the duration in a human readable format with the `-d` argument. The minimum duration is 1 minute.

For example, running a test for 1 hour and 30 minutes:

```
$ cassandra-easy-stress run KeyValue -d "1h 30m"
```

To run a test for 1 day, 3 hours and 15 minutes (why not?):

```
$ cassandra-easy-stress run KeyValue -d "1d 3h 15m"
```

## Partition Keys

A very useful feature is controlling how many partitions are read and written to for a given stress test. Doing a billion operations across a billion partitions is going to have a much different performance profile than writing to one hundred partitions, especially when mixed with different compaction settings. Using `-p` we can control how many partition keys a stress test will leverage. The keys are randomly chosen at the moment.

## Read Rate

It's possible to specify the read rate of a test as a double. For example, if you want to use 1% reads, you'd specify `-r .01`. The sum of the read rate and delete rate must be less than or equal to 1.0.

## Delete Rate

It's possible to specify the delete rate of a test as a double. For example, if you want to use 1% deletes, you'd specify `--deleterate .01`. The sum of the read rate and delete rate must be less than or equal to 1.0.

## Compaction

It's possible to change the compaction strategy used with the `--compaction` flag. At the moment this changes the compaction strategy of every table in the test. This will be addressed in the future to be more flexible.

The `--compaction` flag can accept a raw string along these lines:

```
--compaction "{'class':'LeveledCompactionStrategy'}"
```

Alternatively, a shortcut format exists as of version 2.0:

```
--compaction lcs
```

The following shorthand formats are available:

| Syntax | Expansion |
|--------|-----------|
| stcs | `{'class':'SizeTieredCompactionStrategy'}` |
| stcs,4,32 | `{'class':'SizeTieredCompactionStrategy', 'min_threshold':4, 'max_threshold':32}` |
| lcs | `{'class':'LeveledCompactionStrategy'}` |
| lcs,160 | `{'class':'LeveledCompactionStrategy', 'sstable_size_in_mb':'160'}` |
| lcs,160,10 | `{'class':'LeveledCompactionStrategy', 'sstable_size_in_mb':'160', 'fanout_size':10}` |
| twcs | `{'class':'TimeWindowCompactionStrategy'}` |
| twcs,1,days | `{'class':'TimeWindowCompactionStrategy', 'compaction_window_size':'1', 'compaction_window_unit':'DAYS'}` |
| ucs | `{'class':'UnifiedCompactionStrategy'}` |
| ucs,L4 | `{'class':'UnifiedCompactionStrategy', 'scaling_parameters':'L4'}` |
| ucs,L4,L10 | `{'class':'UnifiedCompactionStrategy', 'scaling_parameters':'L4,L10'}` |

## Compression

It's possible to change the compression options used. At the moment this changes the compression options of every table in the test. This will be addressed in the future to be more flexible.

## Customizing Fields

To some extent, workloads can be customized by leveraging the `--fields` flag. For instance, if we look at the KeyValue workload, we have a table called `keyvalue` which has a `value` field.

To customize the data we use for this field, we provide a generator at the command line. By default, the `value` field will use 100-200 characters of random text. What if we're storing blobs of text instead? Ideally we'd like to tweak this workload to be closer to our production use case. Let's use random sections from various books:

```
$ cassandra-easy-stress run KeyValue --field.keyvalue.value='book(20,40)'
```

Instead of using random strings of garbage, the KeyValue workload will now use 20-40 words extracted from books.

There are other generators available, such as names, gaussian numbers, and cities. Not every generator applies to every type. It's up to the workload to specify which fields can be used this way.

## Workload Restrictions

The `BasicTimeSeries` workload only supports Cassandra versions 3.0 and above. This is because range deletes are used by this workload during runtime. Range deletes are only supported in Cassandra versions 3.0. An exception will be thrown if this workload is used and a Cassandra version less than 3.0 is detected during runtime.
