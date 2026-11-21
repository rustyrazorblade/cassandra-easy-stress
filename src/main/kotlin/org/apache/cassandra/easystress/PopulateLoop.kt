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

import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.datastax.oss.driver.api.core.cql.BatchStatement
import com.datastax.oss.driver.api.core.cql.BatchType
import com.datastax.oss.driver.api.core.cql.Statement
import com.google.common.base.Throwables
import org.apache.cassandra.easystress.PopulateRetryPolicy.Companion.POPULATE_PROFILE
import org.apache.cassandra.easystress.workloads.IStressRunner
import org.apache.cassandra.easystress.workloads.Operation
import org.apache.logging.log4j.kotlin.logger
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

/**
 * How one runner thread populates.
 *
 * @property rows rows to write, deletes included
 * @property batchSize rows written to a partition in one batch
 * @property deleteRatio chance, from 0 to 1, that a write is a delete
 * @property concurrency writes allowed in flight at once
 */
data class PopulateSettings(
    val rows: Long,
    val batchSize: Int,
    val deleteRatio: Double,
    val concurrency: Int,
)

/**
 * Writes the populate rows for one runner thread.
 *
 * The main phase pushes work through [RequestQueue] on a schedule.  That suits a load test, but the
 * queue drops work when it is full, and populate has to write every row.  So this loop pulls the
 * next row only when a write slot is free.  A semaphore holds one permit per write in flight, and a
 * permit comes back only when the driver is done with the write, retries included.
 *
 * @param send executes a statement; the seam tests use in place of a session
 * @param collect records each operation's result for the collectors
 */
class PopulateLoop(
    private val runner: IStressRunner,
    private val keys: Iterator<PartitionKey>,
    private val settings: PopulateSettings,
    private val metrics: Metrics,
    private val send: (Statement<*>) -> CompletionStage<AsyncResultSet>,
    private val collect: (Operation, Either<AsyncResultSet, Throwable>, Long, Long) -> Unit,
) {
    companion object {
        val log = logger()
    }

    /**
     * Writes [PopulateSettings.rows] rows and returns once every write has completed.
     */
    fun run() {
        val permits = Semaphore(settings.concurrency)
        var rowsSent = 0L

        try {
            while (rowsSent < settings.rows) {
                permits.acquire()
                val (operations, statement) =
                    try {
                        val operations = nextOperations(keys.next(), settings.rows - rowsSent)
                        operations to toStatement(operations)
                    } catch (e: Throwable) {
                        // nothing was sent, so nothing else will give this permit back
                        permits.release()
                        throw e
                    }
                rowsSent += operations.size

                val startNanos = System.nanoTime()
                sendSafely(statement).whenComplete { result, error ->
                    try {
                        onComplete(operations, result, error, startNanos)
                    } finally {
                        permits.release()
                    }
                }
            }
        } finally {
            // Every write holds its permit until it completes, so holding all of them means none
            // are in flight.  This also runs when the workload throws, so no write outlives run().
            permits.acquire(settings.concurrency)
            permits.release(settings.concurrency)
        }
    }

    private fun nextOperations(
        key: PartitionKey,
        remaining: Long,
    ): List<Operation> {
        // A delete goes alone.  In a batch it shares the batch timestamp, and a conditional delete
        // would make the whole batch conditional, so a failed condition would drop the rows with it.
        if (settings.deleteRatio > 0.0 && ThreadLocalRandom.current().nextDouble() < settings.deleteRatio) {
            return listOf(runner.getNextDelete(key))
        }
        val count = minOf(settings.batchSize.toLong(), remaining).toInt()
        return List(count) { runner.getNextPopulate(key) }
    }

    private fun toStatement(operations: List<Operation>): Statement<*> {
        val bound = operations.map { requireNotNull(it.bound) { "Populate only supports bound statements, got $it" } }

        // Populate statements are idempotent so the driver hands their write timeouts to PopulateRetryPolicy.
        // Cassandra accepts counter updates in an unlogged batch; only a logged batch rejects them.
        return if (bound.size == 1) {
            bound.first().setExecutionProfileName(POPULATE_PROFILE).setIdempotent(true)
        } else {
            BatchStatement
                .newInstance(BatchType.UNLOGGED, bound)
                .setExecutionProfileName(POPULATE_PROFILE)
                .setIdempotent(true)
        }
    }

    /**
     * Turns an exception thrown while sending into a failed write, so its permit still comes back.
     */
    private fun sendSafely(statement: Statement<*>): CompletionStage<AsyncResultSet> =
        try {
            send(statement)
        } catch (e: Exception) {
            CompletableFuture.failedFuture(e)
        }

    private fun onComplete(
        operations: List<Operation>,
        result: AsyncResultSet?,
        error: Throwable?,
        startNanos: Long,
    ) {
        val endNanos = System.nanoTime()
        val rows = operations.size.toLong()

        if (error != null) {
            val cause = Throwables.getRootCause(error)
            metrics.populateErrors.mark(rows)
            operations.forEach { collect(it, Either.Right(cause), startNanos, endNanos) }
            log.error { "Populate write of $rows rows failed after retries: $cause" }
            return
        }

        metrics.populate.update(endNanos - startNanos, TimeUnit.NANOSECONDS)
        metrics.populateRows.mark(rows)
        operations.forEach { op ->
            collect(op, Either.Left(result!!), startNanos, endNanos)
            if (op is Operation.Mutation) {
                runner.onSuccess(op, result)
            }
        }
    }
}
