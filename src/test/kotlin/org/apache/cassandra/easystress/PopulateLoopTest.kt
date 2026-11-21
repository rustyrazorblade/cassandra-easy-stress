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

import com.codahale.metrics.MetricRegistry
import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.datastax.oss.driver.api.core.cql.BatchStatement
import com.datastax.oss.driver.api.core.cql.BatchType
import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.datastax.oss.driver.api.core.cql.Statement
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.cassandra.easystress.workloads.IStressRunner
import org.apache.cassandra.easystress.workloads.Operation
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread

class PopulateLoopTest {
    private val metrics = Metrics(MetricRegistry(), emptyList(), 0)
    private val result = mockk<AsyncResultSet>()
    private val keys = generateSequence(0L) { it + 1 }.map { PartitionKey("test", it) }

    @AfterEach
    fun shutdown() {
        metrics.shutdown()
    }

    /**
     * The driver's setters return a new statement, so the mock returns itself.
     */
    private fun bound(): BoundStatement =
        mockk<BoundStatement> {
            every { setExecutionProfileName(any()) } returns this
            every { setIdempotent(any()) } returns this
        }

    private fun runner(): IStressRunner {
        val runner = mockk<IStressRunner>(relaxed = true)
        every { runner.getNextPopulate(any()) } answers { Operation.Mutation(bound()) }
        every { runner.getNextDelete(any()) } answers { Operation.Deletion(bound()) }
        return runner
    }

    private fun loop(
        settings: PopulateSettings,
        send: (Statement<*>) -> CompletionStage<AsyncResultSet>,
        runner: IStressRunner = runner(),
    ) = PopulateLoop(runner, keys.iterator(), settings, metrics, send) { _, _, _, _ -> }

    private fun rowsIn(statement: Statement<*>) = if (statement is BatchStatement) statement.size() else 1

    private fun waitUntil(condition: () -> Boolean) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        while (!condition()) {
            check(System.nanoTime() < deadline) { "Timed out waiting for condition" }
            Thread.sleep(5)
        }
    }

    @Test
    fun writesEveryRowInBatchesOfTheConfiguredSize() {
        val sent = ConcurrentLinkedQueue<Statement<*>>()

        loop(PopulateSettings(rows = 10, batchSize = 3, deleteRatio = 0.0, concurrency = 4), {
            sent.add(it)
            CompletableFuture.completedFuture(result)
        }).run()

        assertThat(sent.map { rowsIn(it) }).containsExactly(3, 3, 3, 1)
        assertThat(metrics.populateRows.count).isEqualTo(10)
        assertThat(metrics.populateErrors.count).isZero()
        assertThat(metrics.populate.count).isEqualTo(4)
    }

    @Test
    fun marksEveryStatementIdempotentInThePopulateProfile() {
        val sent = ConcurrentLinkedQueue<Statement<*>>()

        loop(PopulateSettings(rows = 4, batchSize = 2, deleteRatio = 0.0, concurrency = 1), {
            sent.add(it)
            CompletableFuture.completedFuture(result)
        }).run()

        assertThat(sent).hasSize(2).allSatisfy { statement ->
            assertThat(statement.isIdempotent).isTrue()
            assertThat(statement.executionProfileName).isEqualTo(PopulateRetryPolicy.POPULATE_PROFILE)
        }
    }

    /**
     * Counter updates go in the same unlogged batch; Cassandra only rejects them in a logged one.
     * PopulateTest.counterBatchesApplyEveryIncrement checks that against a real server.
     */
    @Test
    fun usesAnUnloggedBatch() {
        val sent = ConcurrentLinkedQueue<Statement<*>>()

        loop(PopulateSettings(rows = 2, batchSize = 2, deleteRatio = 0.0, concurrency = 1), {
            sent.add(it)
            CompletableFuture.completedFuture(result)
        }).run()

        assertThat((sent.single() as BatchStatement).batchType).isEqualTo(BatchType.UNLOGGED)
    }

    @Test
    fun neverHasMoreWritesInFlightThanTheConcurrency() {
        val pending = ConcurrentLinkedQueue<CompletableFuture<AsyncResultSet>>()
        val sends = AtomicInteger()
        val worker =
            thread {
                loop(PopulateSettings(rows = 20, batchSize = 1, deleteRatio = 0.0, concurrency = 3), {
                    sends.incrementAndGet()
                    CompletableFuture<AsyncResultSet>().also { pending.add(it) }
                }).run()
            }

        waitUntil { sends.get() == 3 }
        Thread.sleep(100)
        assertThat(sends.get()).isEqualTo(3)

        pending.poll().complete(result)
        waitUntil { sends.get() == 4 }
        Thread.sleep(100)
        assertThat(sends.get()).isEqualTo(4)

        while (worker.isAlive) {
            pending.poll()?.complete(result)
        }
        assertThat(metrics.populateRows.count).isEqualTo(20)
    }

    @Test
    fun returnsOnlyAfterEveryWriteHasCompleted() {
        val pending = ConcurrentLinkedQueue<CompletableFuture<AsyncResultSet>>()
        val worker =
            thread {
                loop(PopulateSettings(rows = 5, batchSize = 1, deleteRatio = 0.0, concurrency = 10), {
                    CompletableFuture<AsyncResultSet>().also { pending.add(it) }
                }).run()
            }

        // every row is sent, since the concurrency is above the row count
        waitUntil { pending.size == 5 }
        Thread.sleep(100)
        assertThat(worker.isAlive).isTrue()

        pending.forEach { it.complete(result) }
        worker.join(TimeUnit.SECONDS.toMillis(10))
        assertThat(worker.isAlive).isFalse()
        assertThat(metrics.populateRows.count).isEqualTo(5)
    }

    @Test
    fun countsEveryRowOfAFailedBatchAsAnError() {
        val failure = IllegalStateException("write timeout")

        loop(PopulateSettings(rows = 7, batchSize = 3, deleteRatio = 0.0, concurrency = 2), {
            if (rowsIn(it) == 3) CompletableFuture.failedFuture(failure) else CompletableFuture.completedFuture(result)
        }).run()

        assertThat(metrics.populateErrors.count).isEqualTo(6)
        assertThat(metrics.populateRows.count).isEqualTo(1)
    }

    @Test
    fun aSendThatThrowsStillReleasesItsPermit() {
        loop(PopulateSettings(rows = 5, batchSize = 1, deleteRatio = 0.0, concurrency = 1), {
            throw IllegalStateException("session closed")
        }).run()

        assertThat(metrics.populateErrors.count).isEqualTo(5)
    }

    @Test
    fun sendsEachDeleteAloneAndNeverInABatch() {
        val sent = ConcurrentLinkedQueue<Statement<*>>()

        loop(PopulateSettings(rows = 6, batchSize = 3, deleteRatio = 1.0, concurrency = 2), {
            sent.add(it)
            CompletableFuture.completedFuture(result)
        }).run()

        assertThat(sent).hasSize(6).allSatisfy { assertThat(it).isNotInstanceOf(BatchStatement::class.java) }
        assertThat(metrics.populateRows.count).isEqualTo(6)
    }

    @Test
    fun sendsNoDeletesWhenTheRatioIsZero() {
        val runner = runner()

        loop(PopulateSettings(rows = 50, batchSize = 1, deleteRatio = 0.0, concurrency = 4), {
            CompletableFuture.completedFuture(result)
        }, runner).run()

        verify(exactly = 0) { runner.getNextDelete(any()) }
        verify(exactly = 50) { runner.getNextPopulate(any()) }
    }

    /**
     * A workload that throws must not leave writes running after run() returns, because the
     * caller fails the run and closes the session next.
     */
    @Test
    fun aWorkloadThatThrowsWaitsForWritesInFlightThenThrows() {
        val runner = runner()
        val calls = AtomicInteger()
        every { runner.getNextPopulate(any()) } answers {
            if (calls.incrementAndGet() == 3) throw IllegalStateException("workload failed")
            Operation.Mutation(bound())
        }
        val pending = ConcurrentLinkedQueue<CompletableFuture<AsyncResultSet>>()
        val thrown = AtomicReference<Throwable>()
        val worker =
            thread {
                try {
                    loop(PopulateSettings(rows = 10, batchSize = 1, deleteRatio = 0.0, concurrency = 5), {
                        CompletableFuture<AsyncResultSet>().also { pending.add(it) }
                    }, runner).run()
                } catch (e: Throwable) {
                    thrown.set(e)
                }
            }

        waitUntil { pending.size == 2 && calls.get() == 3 }
        Thread.sleep(100)
        assertThat(worker.isAlive).isTrue()

        pending.forEach { it.complete(result) }
        worker.join(TimeUnit.SECONDS.toMillis(10))
        assertThat(worker.isAlive).isFalse()
        assertThat(thrown.get()).hasMessage("workload failed")
        assertThat(metrics.populateRows.count).isEqualTo(2)
    }
}
