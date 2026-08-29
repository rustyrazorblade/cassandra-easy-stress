/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.easystress.commands

import com.beust.jcommander.DynamicParameter
import com.beust.jcommander.Parameter
import com.beust.jcommander.Parameters
import com.beust.jcommander.converters.IParameterSplitter
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.ScheduledReporter
import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DefaultDriverOption
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import com.google.common.base.Preconditions
import com.google.common.util.concurrent.RateLimiter
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import me.tongfei.progressbar.ProgressBar
import me.tongfei.progressbar.ProgressBarStyle
import org.apache.cassandra.easystress.Context
import org.apache.cassandra.easystress.FileReporter
import org.apache.cassandra.easystress.Metrics
import org.apache.cassandra.easystress.PopulateOption
import org.apache.cassandra.easystress.RateLimiterOptimizer
import org.apache.cassandra.easystress.SchemaBuilder
import org.apache.cassandra.easystress.SingleLineConsoleReporter
import org.apache.cassandra.easystress.Workload
import org.apache.cassandra.easystress.WorkloadRunner
import org.apache.cassandra.easystress.collector.Collector
import org.apache.cassandra.easystress.collector.CompositeCollector
import org.apache.cassandra.easystress.collector.HdrCollector
import org.apache.cassandra.easystress.collector.ParquetCollector
import org.apache.cassandra.easystress.converters.ConsistencyLevelConverter
import org.apache.cassandra.easystress.converters.HumanReadableConverter
import org.apache.cassandra.easystress.converters.HumanReadableTimeConverter
import org.apache.cassandra.easystress.generators.ParsedFieldFunction
import org.apache.cassandra.easystress.generators.Registry
import org.apache.logging.log4j.kotlin.logger
import java.io.File
import java.util.Timer
import kotlin.concurrent.fixedRateTimer
import kotlin.concurrent.schedule
import kotlin.concurrent.thread

val DEFAULT_ITERATIONS: Long = 1000000

object ConsistencyLevelSerializer : KSerializer<ConsistencyLevel> {
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("ConsistencyLevel", PrimitiveKind.STRING)

    override fun serialize(
        encoder: Encoder,
        value: ConsistencyLevel,
    ) {
        encoder.encodeString(value.name())
    }

    override fun deserialize(decoder: Decoder): ConsistencyLevel {
        val name = decoder.decodeString()
        return when (name.uppercase()) {
            "ANY" -> ConsistencyLevel.ANY
            "ONE" -> ConsistencyLevel.ONE
            "TWO" -> ConsistencyLevel.TWO
            "THREE" -> ConsistencyLevel.THREE
            "QUORUM" -> ConsistencyLevel.QUORUM
            "ALL" -> ConsistencyLevel.ALL
            "LOCAL_ONE" -> ConsistencyLevel.LOCAL_ONE
            "LOCAL_QUORUM" -> ConsistencyLevel.LOCAL_QUORUM
            "EACH_QUORUM" -> ConsistencyLevel.EACH_QUORUM
            "LOCAL_SERIAL" -> ConsistencyLevel.LOCAL_SERIAL
            "SERIAL" -> ConsistencyLevel.SERIAL
            else -> throw IllegalArgumentException("Unknown consistency level: $name")
        }
    }
}

class NoSplitter : IParameterSplitter {
    override fun split(value: String?): MutableList<String> = mutableListOf(value!!)
}

/**
 * command is the original command that started the program.
 * It's used solely for logging and reporting purposes.
 */
@Serializable
@Parameters(commandDescription = "Run a cassandra-easy-stress profile")
class Run(
    val command: String = "",
) : IStressCommand {
    @Parameter(names = ["--host"])
    var host = System.getenv("CASSANDRA_EASY_STRESS_CASSANDRA_HOST") ?: "127.0.0.1"

    @Parameter(names = ["--port"], description = "Override the cql port. Defaults to 9042.")
    var cqlPort = 9042

    @Parameter(names = ["--username", "-U"])
    var username = "cassandra"

    @Parameter(names = ["--password", "-P"])
    var password = "cassandra"

    @Parameter(required = true)
    var workload = ""

    @Parameter(
        names = ["--compaction"],
        description =
            "Compaction option to use.  Double quotes will auto convert to single for convenience.  " +
                "A shorthand is also available: stcs, lcs, twcs.  See the full documentation for all possibilities.",
    )
    var compaction = ""

    @Parameter(names = ["--compression"], description = "Compression option to pass to CREATE TABLE. Example: {'class':'ZstdCompressor'}")
    var compression = ""

    /**
     * Keyspace to use for stress testing.
     *
     * Default changed from "easy_cass_stress" to "cassandra_easy_stress" to align
     * with the project naming conventions. Users upgrading from previous versions
     * should specify --keyspace explicitly to maintain backward compatibility.
     */
    @Parameter(names = ["--keyspace"], description = "Keyspace to use.  Automatically created.  Use replication option to adjust options.")
    var keyspace = "cassandra_easy_stress"

    @Parameter(
        names = ["--id"],
        description =
            "Identifier for this run, will be used in partition keys.  " +
                "Make unique for when starting concurrent runners.",
    )
    var id = "001"

    @Parameter(
        names = ["--partitions", "-p"],
        description = "Max value of integer component of first partition key.",
        converter = HumanReadableConverter::class,
    )
    var partitionCount = 1000000L

//    @Parameter(names = ["--sample", "-s"], description = "Sample Rate (0-1)")
//    var sampleRate : Double? = null // .1%..  this might be better as a number, like a million.  reasonable to keep in memory

    @Parameter(
        names = ["--readrate", "--reads", "-r"],
        description = "Read Rate, 0-1.  Workloads may have their own defaults.  Default is dependent on workload.",
    )
    var readRate: Double? = null

    @Deprecated("Deprecated, use --rate for more predictable testing.")
    @Parameter(
        names = ["--concurrency", "-c"],
        description = "DEPRECATED.  Concurrent queries allowed.  Increase for larger clusters.  This flag is deprecated and does nothing.",
        converter = HumanReadableConverter::class,
    )
    var concurrency = 100L

    @Parameter(
        names = ["--populate"],
        description = "Pre-population the DB with N rows before starting load test.",
        converter = HumanReadableConverter::class,
    )
    var populate = 0L

    @Parameter(names = ["--threads", "-t"], description = "Threads to run")
    var threads = 1

    @Parameter(
        names = ["--iterations", "-i", "-n"],
        description = "Number of operations to run.",
        converter = HumanReadableConverter::class,
    )
    var iterations: Long = 0

    @Parameter(
        names = ["--duration", "-d"],
        description = "Duration of the stress test.  Expressed in format 1d 3h 15m",
        converter = HumanReadableTimeConverter::class,
    )
    var duration: Long = 0

    @Parameter(names = ["-h", "--help"], description = "Show this help", help = true)
    var help = false

    @Parameter(names = ["--replication"], description = "Replication options")
    var replication = "{'class': 'SimpleStrategy', 'replication_factor':3 }"

    @DynamicParameter(
        names = ["--field."],
        description = "Override a field's data generator.  Example usage: --field.tablename.fieldname='book(100,200)'",
    )
    var fields = mutableMapOf<String, String>()

    @Parameter(names = ["--rate"], description = "Throughput rate, accepts human numbers", converter = HumanReadableConverter::class)
    var rate = 5000L

    @Parameter(names = ["--maxrlat"], description = "Max Read Latency")
    var maxReadLatency: Long? = null

    @Parameter(names = ["--maxwlat"])
    var maxWriteLatency: Long? = null

    @Parameter(names = ["--queue"], description = "Queue Depth.  2x the rate by default.")
    var queueDepth: Long = rate * 2

    @Parameter(names = ["--drop"], description = "Drop the keyspace before starting.")
    var dropKeyspace = false

    @Parameter(
        names = ["--cl"],
        description =
            "Consistency level for reads/writes (Defaults to LOCAL_ONE, " +
                "set custom default with CASSANDRA_EASY_STRESS_CONSISTENCY_LEVEL).",
        converter = ConsistencyLevelConverter::class,
    )
    @Serializable(with = ConsistencyLevelSerializer::class)
    var consistencyLevel =
        System.getenv("CASSANDRA_EASY_STRESS_CONSISTENCY_LEVEL")?.let {
            ConsistencyLevelConverter().convert(it)
        } ?: ConsistencyLevel.LOCAL_ONE

    @Parameter(
        names = ["--scl"],
        description = "Serial consistency level",
        converter = ConsistencyLevelConverter::class,
    )
    @Serializable(with = ConsistencyLevelSerializer::class)
    var serialConsistencyLevel =
        System.getenv("CASSANDRA_EASY_STRESS_SERIAL_CONSISTENCY_LEVEL")?.let {
            ConsistencyLevelConverter().convert(it)
        } ?: ConsistencyLevel.LOCAL_SERIAL

    @Parameter(
        names = ["--cql"],
        description =
            "Additional CQL to run after the schema is created.  " +
                "Use for DDL modifications such as creating indexes.",
        splitter = NoSplitter::class,
    )
    var additionalCQL = mutableListOf<String>()

    @Parameter(
        names = ["--partitiongenerator", "--pg"],
        description = "Method of generating partition keys.  Supports random, normal (gaussian), and sequence.",
    )
    var partitionKeyGenerator: String = "random"

    @Parameter(
        names = ["--coordinatoronly", "--co"],
        description =
            "Coordinator only mode.  This will cause cassandra-easy-stress to round robin between nodes " +
                "without tokens.  Requires using -Djoin_ring=false in cassandra-env.sh.  " +
                "When using this option you must only provide a coordinator to --host.",
    )
    var coordinatorOnlyMode = false

    @Parameter(names = ["--csv"], description = "Write metrics to this file in CSV format.")
    var csvFile = ""

    @Parameter(
        names = ["--parquet"],
        description =
            "Capture client latency metrics to a Apache Parquet file at the specified path.  " +
                "If the file is a directory, the file will be named rawlog.parquet within that directory",
    )
    var parquetFile = ""

    @Parameter(names = ["--paging"], description = "Override the driver's default page size.")
    var paging: Int? = null

    @Parameter(names = ["--paginate"], description = "Paginate through the entire partition before completing")
    var paginate = false

    @Parameter(names = ["--rowcache"], description = "Row cache setting")
    var rowCache = "NONE"

    @Parameter(names = ["--keycache"], description = "Key cache setting")
    var keyCache = "ALL"

    @Parameter(
        names = ["--prometheusport"],
        description = """Override the default prometheus port.  
            Set the default with CASSANDRA_EASY_STRESS_PROM_PORT, or set to 0 to disable.""",
    )
    var prometheusPort = System.getenv("CASSANDRA_EASY_STRESS_PROM_PORT")?.toInt() ?: 9500

    @Parameter(names = ["--ssl"], description = "Enable SSL")
    var ssl = false

    @Parameter(names = ["--ttl"], description = "Table level TTL, 0 to disable.")
    var ttl: Long = 0

    @Parameter(names = ["--dc"], description = "The data center to which requests should be sent")
    var dc: String = System.getenv("CASSANDRA_EASY_STRESS_DEFAULT_DC") ?: "datacenter1"

    @DynamicParameter(names = ["--workload.", "-w."], description = "Override workload specific parameters.")
    var workloadParameters: Map<String, String> = mutableMapOf()

    @Parameter(names = ["--no-schema"], description = "Skips schema creation")
    var noSchema: Boolean = false

    @Parameter(
        names = ["--deleterate", "--deletes"],
        description = "Deletion Rate, 0-1.  Workloads may have their own defaults.  Default is dependent on workload.",
    )
    var deleteRate: Double? = null

    @Transient
    val log = logger()

    // Expose metrics for external access (e.g., MCP server)
    @Transient
    @Volatile
    var currentMetrics: Metrics? = null

    @Parameter(names = ["--max-requests"], description = "Sets the max requests per connection")
    var maxRequestsPerConnection: Int = 32768

    @Parameter(names = ["--max-connections"], description = "Sets the number of max connections per host")
    var maxConnections: Int = 8

    @Parameter(names = ["--hdr"], description = "Print HDR Histograms using this prefix")
    var hdrHistogramPrefix = ""

    var useOptimizer = true

    val session by lazy {
        // Build a programmatic config
        var configLoaderBuilder =
            DriverConfigLoader
                .programmaticBuilder()
                // Default consistency levels
                .withString(DefaultDriverOption.REQUEST_CONSISTENCY, consistencyLevel.toString())
                .withString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY, serialConsistencyLevel.toString())
                // connection pooling
                .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, maxConnections)
                .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, maxConnections)
                .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, maxRequestsPerConnection)
                // might not be practical
                .withString(DefaultDriverOption.REQUEST_TIMEOUT, "30s")
                .withString(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, "10s")
                .withString(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, "30s")

        // Configure page size if specified
        if (paging != null) {
            println("Using custom paging size of $paging")
            configLoaderBuilder = configLoaderBuilder.withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, paging!!)
        }

        // Add DC-aware policy if needed
        if (dc.isNotEmpty()) {
            // In v4, DC awareness is now part of the default policy, just need to configure it
            configLoaderBuilder =
                configLoaderBuilder
                    .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, dc)
                    // Remote DCs won't be used at all
                    .withString(DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC, "0")
        }

        // Build the CqlSession
        val sessionBuilder =
            CqlSession
                .builder()
                .addContactPoint(java.net.InetSocketAddress(host, cqlPort))
                .withAuthCredentials(username, password)
                .withConfigLoader(configLoaderBuilder.build())

        // Add SSL if needed
        if (ssl) {
            sessionBuilder.withSslContext(
                javax.net.ssl.SSLContext
                    .getDefault(),
            )
        }

        // Show settings about to be used
        println(
            "Executing $iterations operations with consistency level $consistencyLevel and serial consistency " +
                "level $serialConsistencyLevel",
        )

        // Build the session
        val session = sessionBuilder.build()
        // No post-initialization steps needed with the new driver

        println("Connected to Cassandra cluster.")
        session
    }

    override fun execute() {
        Preconditions.checkArgument(
            !(duration > 0 && iterations > 0L),
            "Duration and iterations shouldn't be both set at the same time. Please pick just one.",
        )
        // apply the default if the number of iterations wasn't set
        iterations = if (duration == 0L && iterations == 0L) DEFAULT_ITERATIONS else iterations

        Preconditions.checkArgument(
            partitionKeyGenerator in setOf("random", "normal", "sequence"),
            "Partition generator Supports random, normal, and sequence.",
        )

        val workload = Workload.getWorkloads().get(this@Run.workload) ?: error("${this@Run.workload} profile does not exist")

        // Check the workload parameters exist in the workload before we start testing.
        workloadParameters.forEach { (key) ->
            (
                try {
                    workload.getProperty(key)
                } catch (nsee: java.util.NoSuchElementException) {
                    error(
                        "Workload ${this@Run.workload} has no parameter '$key'. Please run the 'info' command to " +
                            "see the list of available parameters for this workload.",
                    )
                }
            )
        }

        /**
         * Check to make sure that readRate + deleteRate <= 1.0 before we start testing.
         *
         * Resolve the final value for the readRate and deleteRate, then check their sum. Note that the readRate is supplied
         * by the profile when the commandline option is unspecified.
         */
        val tmpReadRate = readRate ?: workload.instance.getDefaultReadRate()
        val tmpDeleteRate = deleteRate ?: 0.0

        if ((tmpReadRate + tmpDeleteRate) > 1.0) {
            error(
                "The readRate + deleteRate must be <= 1.0. Values supplied were: readRate = $tmpReadRate and deleteRate = $tmpDeleteRate.",
            )
        }

        createKeyspace()

        val rateLimiter = getRateLimiter()

        workload.applyDynamicSettings(workloadParameters)

        createSchema(workload)
        executeAdditionalCQL()

        val fieldRegistry = createFieldRegistry(workload)

        println("Preparing queries")
        workload.instance.prepare(session)

        // Both of the following are set in the try block, so this is OK
        val metrics = createMetrics()
        currentMetrics = metrics // Store for external access

        // The optimizer steers the rate limiter towards a latency target.  Without a target it has
        // nothing to steer towards, so running it would only cost the user the ramp up.
        val hasLatencyTarget = maxReadLatency != null || maxWriteLatency != null

        if (useOptimizer && !hasLatencyTarget) {
            println(
                "No latency target set, running at a fixed rate of $rate. " +
                    "Set --max-read-latency or --max-write-latency to let the optimizer find the rate.",
            )
        }

        // set up the rate limiter optimizer and put it on a schedule
        var optimizerTimer: Timer? = null
        val optimizer =
            if (useOptimizer && hasLatencyTarget) {
                val opt = RateLimiterOptimizer(rateLimiter, metrics, maxReadLatency, maxWriteLatency)
                opt.reset()

                // Schedule the optimizer to run periodically.  It is a daemon so it can never hold
                // the JVM open, and it is cancelled in the finally block below.
                optimizerTimer =
                    Timer("rate-limiter-optimizer", true).apply {
                        schedule(10000, 5000) {
                            opt.execute()
                        }
                    }
                opt
            } else {
                null
            }

        var runnersExecuted = 0L

        val collector = createCollector()
        val context = Context(session, this, metrics, fieldRegistry, rateLimiter, collector)

        try {
            // run the prepare for each
            val runners = createRunners(workload, context)

            populateData(workload, runners, metrics)
            metrics.resetErrors()
            metrics.resetThroughputTrackers()

            // since the populate workload is going to be substantially different
            // from the test workload, we need to reset the optimizer
            optimizer?.reset()

            metrics.startReporting()
            println("Prometheus metrics are available at http://localhost:$prometheusPort/")

            println("Starting main runner")

            val threads = mutableListOf<Thread>()
            for (runner in runners) {
                val t =
                    thread(start = true, isDaemon = true) {
                        runner.run()
                    }
                runnersExecuted++

                threads.add(t)
            }

            for (t in threads) {
                t.join()
            }

            Thread.sleep(1000)

            // dump out metrics
            for (reporter in metrics.reporters) {
                reporter.report()
            }
        } catch (e: Exception) {
            println(
                "There was an error with cassandra-easy-stress.  Please file a bug at " +
                    "https://github.com/apache/cassandra-easy-stress and report the following exception:\n $e",
            )
            throw e
        } finally {
            // we need to be able to run multiple tests in the same JVM
            // without this cleanup we could have the metrics runner still running and it will cause subsequent tests to fail
            optimizerTimer?.cancel()
            metrics.shutdown()
            currentMetrics = null // Clear reference when done
            collector.close(context)
            session.close()

            Thread.sleep(1000)

            println("Stress complete, $runnersExecuted.")
        }
    }

    private fun createCollector(): Collector {
        val collectors = ArrayList<Collector>()

        if (hdrHistogramPrefix != "") {
            collectors.add(HdrCollector(hdrHistogramPrefix))
        }
        if (parquetFile != "") {
            collectors.add(ParquetCollector(File(parquetFile)))
        }
        if (collectors.size == 1) {
            return collectors[0]
        }
        return CompositeCollector(*collectors.toTypedArray())
    }

    private fun getRateLimiter(): RateLimiter {
        val tmp = RateLimiter.create(rate.toDouble())
        tmp.acquire(rate.toInt())
        return tmp
    }

    /**
     * When we run populate, certain workloads (locking) need to do special things.
     * Like pre-fill every partition with a known dataset.
     * So we have to override populate (and in the case of locking the partition generator).
     * This allows us to do things like sequentially fill every row in the addressable
     * partition space with initial values.
     */
    private fun populateData(
        workload: Workload,
        runners: List<WorkloadRunner>,
        metrics: Metrics,
    ) {
        // The --populate flag can be overridden by the profile

        val option = workload.instance.getPopulateOption(this)
        var deletes = true

        val max =
            when (option) {
                is PopulateOption.Standard -> {
                    populate
                }
                is PopulateOption.Custom -> {
                    println("Using workload specific populate options of ${option.rows}")
                    deletes = option.deletes
                    if (!deletes) {
                        println("Not doing deletes in populate phase")
                    }
                    option.rows
                }
            }

        println("Prepopulating data with $max records per thread ($threads)")

        if (max > 0) {
            ProgressBar("Populate Progress", max * threads, ProgressBarStyle.ASCII).use {
                // update the timer every second, starting 1 second from now, as a daemon thread
                val timer =
                    fixedRateTimer("progress-bar", true, 1000, 1000) {
                        it.stepTo(metrics.populate.count)
                    }

                // calling it on the runner
                val threads = mutableListOf<Thread>()

                runners.forEach {
                    val tmp =
                        thread(start = true, isDaemon = false, name = "populate-X") {
                            it.populate(max, deletes)
                        }
                    threads.add(tmp)
                }
                Thread.sleep(1000)
                for (thread in threads) {
                    thread.join()
                }
                // have we really reached 100%?
                Thread.sleep(1000)
                // stop outputting the progress bar
                timer.cancel()
                // allow the time to die out
            }
            Thread.sleep(1000)
            println("\nPre-populate complete.")
        }
    }

    private fun createRunners(
        workload: Workload,
        sharedContext: Context,
    ): List<WorkloadRunner> {
        val runners =
            IntRange(0, threads - 1).map {
                // println("Connecting")
                println("Connecting to Cassandra cluster ...")
                val context = sharedContext.stress(it)
                WorkloadRunner.create(context, workload.instance)
            }

        val executed =
            runners
                .parallelStream()
                .map {
                    println("Preparing statements.")
                    it.prepare()
                }.count()

        println("$executed threads prepared.")
        return runners
    }

    private fun createMetrics(): Metrics {
        println("Initializing metrics")
        val registry = MetricRegistry()

        val reporters = mutableListOf<ScheduledReporter>()

        if (csvFile.isNotEmpty()) {
            reporters.add(FileReporter(registry, csvFile, command))
        }
        reporters.add(SingleLineConsoleReporter(registry))

        return Metrics(registry, reporters, prometheusPort)
    }

    private fun createFieldRegistry(workload: Workload): Registry {
        val fieldRegistry = Registry.create()

        for ((field, generator) in workload.instance.getFieldGenerators()) {
            log.info("Defaulting $field to $generator")
            fieldRegistry.setDefault(field, generator)
        }

        // Fields  is the raw --fields
        for ((field, generator) in fields) {
            println("$field, $generator")
            // Parsed field, switch this to use the new parser
            // val instance = Registry.getInstance(generator)
            val instance = ParsedFieldFunction(generator)

            val parts = field.split(".")
            val table = parts[0]
            val fieldName = parts[1]
//            val
            // TODO check to make sure the fields exist
            fieldRegistry.setOverride(table, fieldName, instance)
        }
        return fieldRegistry
    }

    private fun createKeyspace() {
        if (noSchema) {
            println("Skipping keyspace creation")
        } else {
            if (dropKeyspace) {
                println("Dropping $keyspace")
                session.execute("DROP KEYSPACE IF EXISTS $keyspace")
                Thread.sleep(5000)
            }

            val createKeyspace =
                """CREATE KEYSPACE
            | IF NOT EXISTS $keyspace
            | WITH replication = $replication
                """.trimMargin()

            println("Creating $keyspace: \n$createKeyspace\n")
            session.execute(createKeyspace)
        }
        session.execute("USE $keyspace")
    }

    private fun executeAdditionalCQL() {
        // run additional CQL
        for (statement in additionalCQL) {
            println(statement)
            session.execute(statement)
        }
    }

    fun createSchema(workload: Workload) {
        println("Creating Tables")

        if (noSchema) return

        for (statement in workload.instance.schema()) {
            val s =
                SchemaBuilder
                    .create(statement)
                    .withCompaction(compaction)
                    .withCompression(compression)
                    .withRowCache(rowCache)
                    .withKeyCache(keyCache)
                    .withDefaultTTL(ttl)
                    .build()
            println(s)
            session.execute(s)
        }
    }
}
