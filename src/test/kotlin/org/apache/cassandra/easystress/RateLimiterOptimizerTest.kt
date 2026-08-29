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

import com.google.common.util.concurrent.RateLimiter
import io.mockk.every
import io.mockk.junit5.MockKExtension
import io.mockk.mockk
import io.mockk.spyk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.util.Optional
import java.util.concurrent.TimeUnit

@ExtendWith(MockKExtension::class)
class RateLimiterOptimizerTest {
    var rateLimiter: RateLimiter = RateLimiter.create(1000.0)
    val metrics = mockk<Metrics>()

    fun pair(
        current: Double,
        max: Long,
    ): Optional<Pair<Double, Long>> = Optional.of(Pair(current, max))

    @Test
    fun testSimpleReadLimitRaise() {
        val optimizer = spyk(RateLimiterOptimizer(rateLimiter, metrics, 100, 100, isStepPhase = false))
        every { optimizer.getCurrentAndMaxLatency() } returns pair(10.0, 50)
        every { optimizer.getTotalOperations() } returns 100
        every { optimizer.getCurrentTotalThroughput() } returns 1000.0

        val newRate = optimizer.execute()
        assertThat(newRate).isGreaterThan(1000.0)
    }

    @Test
    fun testSimpleLimitLower() {
        val maxLatency = 100L
        // the original rate limit is 1K, so the test here is that we're over our max latency
        // and we should see the rate limiter be < 1K.
        val optimizer = spyk(RateLimiterOptimizer(rateLimiter, metrics, maxLatency, maxLatency, isStepPhase = false))
        every { optimizer.getCurrentAndMaxLatency() } returns pair(110.0, maxLatency)
        every { optimizer.getTotalOperations() } returns 100
        every { optimizer.getCurrentTotalThroughput() } returns 1000.0

        val newRate = optimizer.execute()
        assertThat(newRate).isLessThan(1000.0)
    }

    // Current limiter: 10.0 latency 1.4934458E7, max: 50 adjustment factor: 2.5109716067365823E-6
    @Test
    fun testLowInitialRate() {
        val maxLatency = 50L
        rateLimiter = RateLimiter.create(10.0)

        val optimizer = spyk(RateLimiterOptimizer(rateLimiter, metrics, maxLatency, maxLatency, isStepPhase = false))
        every { optimizer.getCurrentAndMaxLatency() } returns pair(1.0, maxLatency)
        every { optimizer.getTotalOperations() } returns 100
        every { optimizer.getCurrentTotalThroughput() } returns 10.0

        val newRate = optimizer.execute()
        assertThat(newRate).isGreaterThan(10.0)
    }

    /**
     * Under overload the client falls behind its own limit, so the limit constrains nothing.
     * Cutting the limit has to bring it below what the client is actually achieving.
     */
    @Test
    fun testReducesAgainstAchievedThroughput() {
        val maxLatency = 100L
        val optimizer = spyk(RateLimiterOptimizer(rateLimiter, metrics, maxLatency, maxLatency, isStepPhase = false))
        every { optimizer.getCurrentAndMaxLatency() } returns pair(200.0, maxLatency)
        every { optimizer.getTotalOperations() } returns 100

        // the limit is 1000, but the client is only managing 400 ops/sec
        every { optimizer.getCurrentTotalThroughput() } returns 400.0

        val newRate = optimizer.execute()

        // 400 * 0.9, not 1000 * 0.9
        assertThat(newRate).isEqualTo(360.0)
    }

    /**
     * The throughput tracker reports zero until it has warmed up.  That must not be read as a
     * client managing no operations at all.
     */
    @Test
    fun testIgnoresAColdThroughputTracker() {
        val maxLatency = 100L
        val optimizer = spyk(RateLimiterOptimizer(rateLimiter, metrics, maxLatency, maxLatency, isStepPhase = false))
        every { optimizer.getCurrentAndMaxLatency() } returns pair(200.0, maxLatency)
        every { optimizer.getTotalOperations() } returns 100
        every { optimizer.getCurrentTotalThroughput() } returns 0.0

        val newRate = optimizer.execute()

        // one ordinary reduction against the current rate, not a drop to the floor
        assertThat(newRate).isEqualTo(900.0)
    }

    /**
     * A run of reductions must not drive the rate arbitrarily close to zero.
     */
    @Test
    fun testRateHasAFloor() {
        val maxLatency = 100L
        val optimizer = spyk(RateLimiterOptimizer(rateLimiter, metrics, maxLatency, maxLatency, isStepPhase = false))
        every { optimizer.getCurrentAndMaxLatency() } returns pair(500.0, maxLatency)
        every { optimizer.getTotalOperations() } returns 100
        every { optimizer.getCurrentTotalThroughput() } returns 1.0

        val newRate = optimizer.execute()

        // the starting rate is 1000, so the floor is 10
        assertThat(newRate).isEqualTo(10.0)
    }

    /**
     * Acting again before the latency window has passed means acting on the previous rate, which
     * compounds every adjustment.
     */
    @Test
    fun testWaitsForTheLatencyWindowBeforeChangingAgain() {
        val maxLatency = 100L
        var now = 1_000_000L
        val optimizer =
            spyk(
                RateLimiterOptimizer(
                    rateLimiter,
                    metrics,
                    maxLatency,
                    maxLatency,
                    isStepPhase = false,
                    clock = { now },
                ),
            )
        every { optimizer.getCurrentAndMaxLatency() } returns pair(110.0, maxLatency)
        every { optimizer.getTotalOperations() } returns 100
        every { optimizer.getCurrentTotalThroughput() } returns 1000.0

        val firstRate = optimizer.execute()
        assertThat(firstRate).isEqualTo(900.0)

        // no time has passed, so the metrics still describe the old rate
        assertThat(optimizer.execute()).isEqualTo(900.0)

        // one full window later the optimizer may act again
        now += TimeUnit.SECONDS.toMillis(Metrics.LATENCY_WINDOW_SECONDS)
        assertThat(optimizer.execute()).isLessThan(900.0)
    }

    /**
     * The increase depends on the fraction of the latency budget still unused, not on the size of
     * the budget.  The same fraction has to produce the same increase at any target.
     */
    @Test
    fun testIncreaseDoesNotDependOnTheSizeOfTheTarget() {
        val optimizer = RateLimiterOptimizer(rateLimiter, metrics, 100, 100, isStepPhase = false)

        val smallTarget = optimizer.getNextValue(1000.0, 1000.0, 10.0, 100)
        val largeTarget = optimizer.getNextValue(1000.0, 1000.0, 100.0, 1000)

        assertThat(smallTarget).isEqualTo(largeTarget)
    }
}
