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
package org.apache.cassandra.easystress

import com.datastax.oss.driver.api.core.cql.SimpleStatement
import org.apache.cassandra.easystress.workloads.IStressRunner
import org.apache.cassandra.easystress.workloads.IStressWorkload
import org.apache.cassandra.easystress.workloads.Operation
import org.apache.logging.log4j.kotlin.logger
import java.time.Duration
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.time.format.FormatStyle

class PartitionKeyGeneratorException : Exception()

/**
 * Single threaded profile runner.
 * One profile runner should be created per thread
 * Logs all errors along the way
 * Keeps track of useful metrics, per thread
 */
class WorkloadRunner(
    val context: StressContext,
    val profile: IStressWorkload,
    val partitionKeyGenerator: PartitionKeyGenerator,
) {
    companion object {
        fun create(
            context: StressContext,
            profile: IStressWorkload,
        ): WorkloadRunner {
            val partitionKeyGenerator = getGenerator(context, context.mainArguments.partitionKeyGenerator)

            return WorkloadRunner(context, profile, partitionKeyGenerator)
        }

        /**
         * Partition key generators by the name --partitiongenerator and --populate.generator accept.
         */
        val GENERATORS: Map<String, (String) -> PartitionKeyGenerator> =
            mapOf(
                "normal" to { prefix -> PartitionKeyGenerator.normal(prefix) },
                "random" to { prefix -> PartitionKeyGenerator.random(prefix) },
                "sequence" to { prefix -> PartitionKeyGenerator.sequence(prefix) },
            )

        fun getGenerator(
            context: StressContext,
            name: String,
        ): PartitionKeyGenerator {
            val prefix = context.mainArguments.id + "." + context.thread + "."
            println("Creating generator $name")
            val create = GENERATORS[name] ?: throw PartitionKeyGeneratorException()
            return create(prefix)
        }

        val log = logger()
    }

    val readRate: Double

    init {
        val tmp = context.mainArguments.readRate

        if (tmp != null) {
            readRate = tmp
        } else {
            readRate = profile.getDefaultReadRate()
        }

        // check unsupported operations
    }

    val deleteRate: Double

    init {
        val tmp = context.mainArguments.deleteRate

        if (tmp != null) {
            deleteRate = tmp
        } else {
            deleteRate = 0.0
        }
    }

    /**
     * One runner for both populate and the main phase.  Workloads such as LWT record what they
     * wrote in the runner, so the main phase has to see the state populate left behind.  It is
     * created on first use, because creating it can start work, such as CreateDrop's DDL timer.
     */
    private val runner: IStressRunner by lazy { profile.getRunner(context) }

    fun print(message: String) {
        println("[Thread ${context.thread}]: $message")
    }

    /**

     */
    fun run() {
        if (context.mainArguments.duration == 0L) {
            print("Running the profile for ${context.mainArguments.iterations} iterations...")
        } else {
            val startTime = LocalTime.now()
            val endTime = startTime.plus(Duration.ofSeconds(context.mainArguments.duration))
            val formatter = DateTimeFormatter.ofLocalizedTime(FormatStyle.SHORT)

            val durationDisplay =
                if (context.mainArguments.duration < 60) {
                    "${context.mainArguments.duration}s"
                } else {
                    "${context.mainArguments.duration / 60}m ${context.mainArguments.duration % 60}s"
                }

            print(
                "Running the profile for $durationDisplay (start: ${formatter.format(
                    startTime,
                )} end: ${formatter.format(endTime)})",
            )
        }
        executeOperations(context.mainArguments.iterations, context.mainArguments.duration)
    }

    /**
     * Used for both pre-populating data and for performing the actual runner
     */
    private fun executeOperations(
        iterations: Long,
        duration: Long,
    ) {
        // we use MAX_VALUE since it's essentially infinite if we give a duration
        val totalValues = if (duration > 0) Long.MAX_VALUE else iterations

        val queue = RequestQueue(partitionKeyGenerator, context, totalValues, duration, runner, readRate, deleteRate)

        queue.start()

        // pull requests off the queue instead of using generateKey
        // move the getNextOperation into the queue thing
        val paginate = context.mainArguments.paginate
        for (op in queue.getNextOperation()) {
            // In driver v4, async execution returns a CompletionStage
            val startNanos = System.nanoTime()
            val future =
                when (op) {
                    is Operation.DDL -> {
                        // Create a SimpleStatement for DDL operations
                        context.session.executeAsync(SimpleStatement.newInstance(op.statement!!))
                    }
                    else -> {
                        // Ensure bound statement is not null
                        context.session.executeAsync(op.bound!!)
                    }
                }

            // with pagination the operation completes after the last page arrives
            val completed = if (paginate) future.thenCompose { it.fetchAllPages() } else future

            completed.whenComplete(OperationCallback(context, runner, op, startNanos))
        }
    }

    /**
     * Prepopulates the database for this thread.
     * Does not count towards the normal metrics: latency goes to the populateMutations timer
     * and rows to populateRows, so it can (and should) be graphed separately.
     *
     * @param generator name of the partition key generator, see [getGenerator]
     */
    fun populate(
        settings: PopulateSettings,
        generator: String,
    ) {
        val keys = getGenerator(context, generator).generateKey(Long.MAX_VALUE, context.mainArguments.partitionCount)

        PopulateLoop(
            runner,
            keys.iterator(),
            settings,
            context.metrics,
            { context.session.executeAsync(it) },
            { op, result, startNanos, endNanos -> context.collect(op, result, startNanos, endNanos) },
        ).run()
    }

    fun prepare() {
        profile.prepare(context.session)
    }
}
