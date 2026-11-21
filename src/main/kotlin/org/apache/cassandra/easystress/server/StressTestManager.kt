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
package org.apache.cassandra.easystress.server

import io.modelcontextprotocol.kotlin.sdk.server.RegisteredTool
import io.modelcontextprotocol.kotlin.sdk.types.CallToolRequest
import io.modelcontextprotocol.kotlin.sdk.types.CallToolResult
import io.modelcontextprotocol.kotlin.sdk.types.EmptyJsonObject
import io.modelcontextprotocol.kotlin.sdk.types.TextContent
import io.modelcontextprotocol.kotlin.sdk.types.Tool
import io.modelcontextprotocol.kotlin.sdk.types.ToolSchema
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject
import org.apache.cassandra.easystress.commands.Run
import org.apache.logging.log4j.kotlin.logger
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread

/**
 * Manages shared state for stress test execution across MCP tools.
 *
 * This class provides thread-safe coordination for running stress tests, ensuring that:
 * - Only one stress test runs at a time
 * - Tools can query current test status and metrics
 * - Proper cleanup occurs when tests complete or are stopped
 *
 * State Management:
 * - Uses atomic operations to prevent race conditions
 * - Maintains current test metadata (command, thread, start time)
 * - Tracks execution status ("idle", "running", "completed", "failed: <reason>")
 *
 * Thread Safety: All methods are thread-safe through atomic operations.
 * Multiple tools can safely query or modify state concurrently.
 *
 * Typical lifecycle:
 * 1. RunTool calls tryStart(runCommand) which starts test in background thread
 * 2. StatusTool queries current state via getCurrentCommand()/getStatus()
 * 3. StopTool calls stop() to terminate running test
 * 4. Test completion automatically calls stop() to release lock
 */
class StressTestManager {
    private val logger = logger()
    private val running = AtomicBoolean(false)
    private val currentThread = AtomicReference<Thread?>()
    private val currentCommand = AtomicReference<Run?>()
    private val startTime = AtomicReference<Long?>()
    private val status = AtomicReference<String>("idle")
    private val nextJobId = AtomicInteger(1)
    private val currentJobId = AtomicReference<String?>()

    /**
     * Checks if a stress test is currently running.
     *
     * @return true if a test is running, false otherwise
     */
    fun isRunning(): Boolean = running.get()

    /**
     * Gets the thread executing the current stress test.
     *
     * @return The stress test thread, or null if no test is running
     */
    fun getCurrentThread(): Thread? = currentThread.get()

    /**
     * Gets the Run command for the current stress test.
     *
     * Provides access to test configuration and real-time metrics.
     *
     * @return The current Run command, or null if no test is running
     */
    fun getCurrentCommand(): Run? = currentCommand.get()

    /**
     * Gets the Unix timestamp (milliseconds) when the current test started.
     *
     * @return Start time in milliseconds, or null if no test is/was running
     */
    fun getStartTime(): Long? = startTime.get()

    /**
     * Gets the current execution status.
     *
     * Possible values:
     * - "idle": No test running
     * - "running": Test currently executing
     * - "completed": Last test completed successfully
     * - "stopped": Last test was stopped by user
     * - "failed: <message>": Last test failed with error message
     *
     * @return Current status string
     */
    fun getStatus(): String = status.get()

    /**
     * Gets the current job ID.
     *
     * @return The current job ID string, or null if no job is/was running
     */
    fun getCurrentJobId(): String? = currentJobId.get()

    /**
     * Generates a unique job ID for a stress test run.
     *
     * Job IDs are sequentially incremented integers, zero-padded to a minimum
     * of 3 digits (e.g., "001", "002", ..., "999", "1000", ...).
     *
     * @return A zero-padded job ID string
     */
    private fun generateJobId(): String {
        val id = nextJobId.getAndIncrement()
        return id.toString().padStart(3, '0')
    }

    /**
     * Atomically attempts to start a stress test in a background thread.
     *
     * Uses compare-and-set to ensure only one test can start at a time,
     * preventing race conditions when multiple tools attempt concurrent execution.
     *
     * If successful, starts the stress test in a daemon background thread and records
     * test metadata. The test runs until completion or failure, at which point the
     * lock is released automatically.
     *
     * @param runCommand The Run command to execute
     * @return true if this call acquired exclusive execution rights and started the test,
     *         false if a test is already running
     */
    fun tryStart(runCommand: Run): Boolean {
        if (!running.compareAndSet(false, true)) {
            logger.warn { "Attempted to start stress test while another is already running" }
            return false
        }

        // Generate and assign job ID
        val jobId = generateJobId()
        currentJobId.set(jobId)
        runCommand.id = jobId

        // Record metadata immediately
        val startTimeMs = System.currentTimeMillis()
        startTime.set(startTimeMs)
        currentCommand.set(runCommand)
        status.set("running")
        logger.info {
            "Starting stress test $jobId with workload=${runCommand.workload}, host=${runCommand.host}, threads=${runCommand.threads}"
        }

        // Start background thread to execute the test
        val testThread =
            thread(start = true, isDaemon = true, name = "stress-test-runner") {
                try {
                    logger.debug { "Executing stress test in background thread" }
                    runCommand.execute()
                    status.set("completed")
                    logger.info { "Stress test completed successfully after ${(System.currentTimeMillis() - startTimeMs) / 1000}s" }
                } catch (e: Exception) {
                    logger.error(e) { "Stress test failed or was interrupted: ${e.message}" }
                    val errorMessage = "failed: ${e.message}"
                    status.set(errorMessage)
                    println("Stress test failed: ${e.message}")
                } finally {
                    stop()
                }
            }

        currentThread.set(testThread)
        logger.debug { "Background thread started: ${testThread.name}" }
        return true
    }

    /**
     * Updates the execution status.
     *
     * Typically called to record "running", "completed", or "failed: <reason>".
     *
     * @param newStatus The new status string
     */
    fun setStatus(newStatus: String) {
        val oldStatus = status.getAndSet(newStatus)
        logger.debug { "Status transition: $oldStatus -> $newStatus" }
    }

    /**
     * Stops the currently running test by interrupting its thread.
     *
     * This method:
     * - Interrupts the test thread if it's alive
     * - Releases the execution lock (sets running=false)
     * - Retains metadata for querying
     *
     * Safe to call multiple times and from any thread.
     * Does not modify the status field - caller should update status if needed.
     */
    fun stop() {
        currentThread.get()?.let { thread ->
            if (thread.isAlive && thread != Thread.currentThread()) {
                logger.info { "Stopping stress test thread: ${thread.name}" }
                thread.interrupt()
            }
        }
        val wasRunning = running.getAndSet(false)
        if (wasRunning) {
            logger.debug { "Stress test execution lock released" }
        }
    }

    /**
     * Clears all test state and metadata.
     *
     * Used for cleanup after test completion or failure.
     * Resets to "idle" state ready for next test.
     */
    fun clear() {
        logger.debug { "Clearing all stress test state" }
        running.set(false)
        currentThread.set(null)
        currentCommand.set(null)
        startTime.set(null)
        status.set("idle")
        currentJobId.set(null)
        logger.info { "Stress test manager reset to idle state" }
    }

    /**
     * Returns a RegisteredTool for running stress tests via MCP.
     *
     * The tool provides an interface for starting stress tests with custom configurations.
     * Tests run in a background thread and can be monitored via the status tool.
     * Only one test can run at a time - concurrent start attempts will be rejected.
     *
     * @return RegisteredTool configured for the "run" operation
     */
    fun getRunTool(): RegisteredTool =
        RegisteredTool(
            tool =
                Tool(
                    name = "run",
                    title = null,
                    description = "Start a stress test with custom configuration",
                    inputSchema = ToolInputSchemaGenerator(Run::class).generateToolInput(),
                    outputSchema = null,
                    annotations = null,
                ),
            handler = { request: CallToolRequest ->
                withContext(Dispatchers.IO) {
                    try {
                        val arguments = request.params.arguments ?: EmptyJsonObject
                        val runCommand = Json.decodeFromJsonElement(Run.serializer(), arguments)
                        if (tryStart(runCommand)) {
                            val jobId = getCurrentJobId() ?: "unknown"
                            println("Job started: $jobId")
                            CallToolResult(
                                content =
                                    listOf(
                                        TextContent(
                                            text =
                                                "Stress test started successfully with job ID: $jobId. " +
                                                    "Use status tool to monitor progress.",
                                        ),
                                    ),
                            )
                        } else {
                            CallToolResult(
                                content =
                                    listOf(
                                        TextContent(
                                            text =
                                                "A stress test is already running. " +
                                                    "Use status tool to check progress or stop tool to terminate it.",
                                        ),
                                    ),
                                isError = true,
                            )
                        }
                    } catch (e: Exception) {
                        CallToolResult(
                            content =
                                listOf(
                                    TextContent(
                                        text = "Error parsing run request: ${e.message}",
                                    ),
                                ),
                            isError = true,
                        )
                    }
                }
            },
        )

    /**
     * Returns a RegisteredTool for stopping stress tests via MCP.
     *
     * The tool provides an interface for stopping currently running stress tests.
     * Interrupts the test thread and releases the execution lock.
     * Returns an error if no test is running.
     *
     * @return RegisteredTool configured for the "stop" operation
     */
    fun getStopTool(): RegisteredTool =
        RegisteredTool(
            tool =
                Tool(
                    name = "stop",
                    title = null,
                    description = "Stop a currently running stress test",
                    inputSchema = ToolSchema(properties = EmptyJsonObject),
                    outputSchema = null,
                    annotations = null,
                ),
            handler = { _: CallToolRequest ->
                withContext(Dispatchers.IO) {
                    if (isRunning()) {
                        stop()
                        setStatus("stopped")
                        CallToolResult(
                            content =
                                listOf(
                                    TextContent(
                                        text = "Stress test stopped successfully. Use status tool to verify.",
                                    ),
                                ),
                            isError = false,
                        )
                    } else {
                        CallToolResult(
                            content =
                                listOf(
                                    TextContent(
                                        text = "No stress test is currently running.",
                                    ),
                                ),
                            isError = true,
                        )
                    }
                }
            },
        )

    /**
     * Returns a RegisteredTool for querying stress test status and metrics via MCP.
     *
     * The tool provides real-time access to:
     * - Test execution status (idle, running, completed, failed)
     * - Throughput metrics (operations per second)
     * - Latency percentiles (mean, median, p95, p99, p999, max)
     * - Error rates and counts
     * - Operation counts
     *
     * @return RegisteredTool configured for the "status" operation
     */
    fun getStatusTool(): RegisteredTool =
        RegisteredTool(
            tool =
                Tool(
                    name = "status",
                    title = null,
                    description = "Get status of running stress tests.  Latency numbers are expressed in microseconds.",
                    inputSchema = ToolSchema(properties = EmptyJsonObject),
                    outputSchema = null,
                    annotations = null,
                ),
            handler = { _: CallToolRequest ->
                withContext(Dispatchers.IO) {
                    val response =
                        buildJsonObject {
                            val currentStatus = getStatus()
                            put("status", currentStatus)
                            put("is_running", isRunning())

                            if (isRunning()) {
                                val thread = getCurrentThread()
                                val command = getCurrentCommand()
                                val startTime = getStartTime()

                                put("message", "Stress test is currently running")
                                thread?.let { put("thread_name", it.name) }
                                thread?.let { put("thread_alive", it.isAlive) }
                                command?.let {
                                    putJsonObject("current_test") {
                                        put("job_id", getCurrentJobId() ?: "unknown")
                                        put("workload", it.workload)
                                        put("host", it.host)
                                        put("iterations", it.iterations)
                                        put("rate", it.rate)
                                        put("threads", it.threads)
                                        put("keyspace", it.keyspace)

                                        // Test Configuration
                                        put("duration", it.duration)
                                        it.readRate?.let { rate -> put("readRate", rate) }
                                        it.deleteRate?.let { rate -> put("deleteRate", rate) }
                                        put("populate", it.populate)
                                        put("partitionKeyGenerator", it.partitionKeyGenerator)
                                        put("partitionValues", it.partitionCount)
                                        put("coordinatorOnlyMode", it.coordinatorOnlyMode)
                                        put("queueDepth", it.queueDepth)
                                        it.maxReadLatency?.let { lat -> put("maxReadLatency", lat) }
                                        it.maxWriteLatency?.let { lat -> put("maxWriteLatency", lat) }

                                        // Schema & Table Settings
                                        put("compaction", it.compaction)
                                        put("compression", it.compression)
                                        put("replication", it.replication)
                                        put("mutationTracking", it.mutationTracking)
                                        put("ttl", it.ttl)
                                        put("rowCache", it.rowCache)
                                        put("keyCache", it.keyCache)
                                        put("dropKeyspace", it.dropKeyspace)
                                        put("noSchema", it.noSchema)

                                        // Consistency & CQL
                                        put("consistencyLevel", it.consistencyLevel.name())
                                        put("serialConsistencyLevel", it.serialConsistencyLevel.name())
                                        if (it.additionalCQL.isNotEmpty()) {
                                            putJsonArray("additionalCQL") {
                                                it.additionalCQL.forEach { cql -> add(JsonPrimitive(cql)) }
                                            }
                                        }

                                        // Query Settings
                                        it.paging?.let { page -> put("paging", page) }
                                        put("paginate", it.paginate)

                                        // Workload Parameters
                                        if (it.fields.isNotEmpty()) {
                                            putJsonObject("fields") {
                                                it.fields.forEach { (key, value) ->
                                                    put(key, value)
                                                }
                                            }
                                        }
                                        if (it.workloadParameters.isNotEmpty()) {
                                            putJsonObject("workloadParameters") {
                                                it.workloadParameters.forEach { (key, value) ->
                                                    put(key, value)
                                                }
                                            }
                                        }
                                    }

                                    // Add real-time metrics if available
                                    it.currentMetrics?.let { metrics ->
                                        putJsonObject("metrics") {
                                            // Throughput metrics (ops/sec)
                                            putJsonObject("throughput") {
                                                put("selects", metrics.getSelectThroughput())
                                                put("mutations", metrics.getMutationThroughput())
                                                put("deletions", metrics.getDeletionThroughput())
                                                put("populate", metrics.getPopulateThroughput())

                                                val totalThroughput =
                                                    metrics.getSelectThroughput() +
                                                        metrics.getMutationThroughput() +
                                                        metrics.getDeletionThroughput()
                                                put("total", totalThroughput)
                                            }

                                            // Latency metrics (using Timer snapshots)
                                            putJsonObject("latency") {
                                                val selectSnapshot = metrics.selects.snapshot
                                                val mutationSnapshot = metrics.mutations.snapshot
                                                val deleteSnapshot = metrics.deletions.snapshot

                                                putJsonObject("selects") {
                                                    put("mean", selectSnapshot.mean)
                                                    put("median", selectSnapshot.median)
                                                    put("p95", selectSnapshot.get95thPercentile())
                                                    put("p99", selectSnapshot.get99thPercentile())
                                                    put("p999", selectSnapshot.get999thPercentile())
                                                    put("max", selectSnapshot.max)
                                                }
                                                putJsonObject("mutations") {
                                                    put("mean", mutationSnapshot.mean)
                                                    put("median", mutationSnapshot.median)
                                                    put("p95", mutationSnapshot.get95thPercentile())
                                                    put("p99", mutationSnapshot.get99thPercentile())
                                                    put("p999", mutationSnapshot.get999thPercentile())
                                                    put("max", mutationSnapshot.max)
                                                }
                                                putJsonObject("deletions") {
                                                    put("mean", deleteSnapshot.mean)
                                                    put("median", deleteSnapshot.median)
                                                    put("p95", deleteSnapshot.get95thPercentile())
                                                    put("p99", deleteSnapshot.get99thPercentile())
                                                    put("p999", deleteSnapshot.get999thPercentile())
                                                    put("max", deleteSnapshot.max)
                                                }
                                            }

                                            // Error metrics
                                            putJsonObject("errors") {
                                                put("count", metrics.errors.count)
                                                put("rate", metrics.errors.meanRate)
                                                put("rate_1min", metrics.errors.oneMinuteRate)
                                                put("rate_5min", metrics.errors.fiveMinuteRate)
                                                put("rate_15min", metrics.errors.fifteenMinuteRate)
                                            }

                                            // Operation counts
                                            putJsonObject("counts") {
                                                put("selects", metrics.selects.count)
                                                put("mutations", metrics.mutations.count)
                                                put("deletions", metrics.deletions.count)
                                                put("populate", metrics.populateRows.count)
                                                put("populate_errors", metrics.populateErrors.count)
                                                put("errors", metrics.errors.count)
                                                put(
                                                    "total",
                                                    metrics.selects.count +
                                                        metrics.mutations.count +
                                                        metrics.deletions.count,
                                                )
                                            }
                                        }
                                    }
                                }
                                startTime?.let {
                                    put("start_time", it)
                                    put("elapsed_seconds", (System.currentTimeMillis() - it) / 1000)
                                }
                            } else {
                                put(
                                    "message",
                                    when (currentStatus) {
                                        "idle" -> "No stress test is currently running"
                                        "completed" -> "Last stress test completed successfully"
                                        "stopped" -> "Last stress test was stopped"
                                        else ->
                                            if (currentStatus.startsWith("failed")) {
                                                "Last stress test failed: ${currentStatus.substring(7)}"
                                            } else {
                                                "Status: $currentStatus"
                                            }
                                    },
                                )
                                getStartTime()?.let { put("last_run_time", it) }
                            }
                        }

                    CallToolResult(
                        content =
                            listOf(
                                TextContent(
                                    text = response.toString(),
                                ),
                            ),
                        isError = false,
                    )
                }
            },
        )
}
