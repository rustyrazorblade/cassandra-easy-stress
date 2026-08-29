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
import org.apache.logging.log4j.kotlin.logger
import java.util.Optional
import java.util.concurrent.TimeUnit

/**
 * Dynamically adjusts the rate limiter based on observed latency metrics.
 *
 * This class implements an adaptive algorithm that:
 * 1. Gradually increases load during an initial "step phase"
 * 2. Monitors latencies to ensure they stay within target thresholds
 * 3. Dynamically adjusts throughput based on current performance metrics
 *
 * The loop only acts once the latency metrics reflect its last change.  See [hasSettled].
 */
class RateLimiterOptimizer(
    val rateLimiter: RateLimiter,
    val metrics: Metrics,
    val maxReadLatency: Long?,
    val maxWriteLatency: Long?,
    var isStepPhase: Boolean = true,
    val clock: () -> Long = System::currentTimeMillis,
) {
    companion object {
        val log = logger()

        /** Cut the rate to this fraction of achieved throughput when latency exceeds the target. */
        const val REDUCTION_FACTOR = 0.90

        /** The largest single increase, applied when latency is far below the target. */
        const val MAX_INCREASE = 0.05

        /** Hold the rate steady above this fraction of the target, so the loop does not hunt. */
        const val DEAD_BAND = 0.90

        /** Never raise a limit the client is not already using. */
        const val MIN_UTILIZATION = 0.90

        /** Wait for this many operations before trusting the latency percentiles. */
        const val MIN_OPERATIONS = 100L

        /** The step phase reaches the target rate in this many steps. */
        const val STEPS = 10

        /** The rate never falls below the starting rate divided by this. */
        const val RATE_FLOOR_DIVISOR = 100.0
    }

    val durationFactor = 1.0 / TimeUnit.MILLISECONDS.toNanos(1)

    val initial: Double = rateLimiter.rate
    val stepValue = initial / STEPS

    /**
     * A floor stops a run of reductions from driving the rate arbitrarily close to zero, which
     * leaves the test stalled with no way back up.
     */
    private val minRate = initial / RATE_FLOOR_DIVISOR

    /**
     * Latency percentiles cover [Metrics.LATENCY_WINDOW_SECONDS].  Acting again before that window
     * has passed means acting on the previous rate, which compounds every adjustment.
     */
    private val settleMs = TimeUnit.SECONDS.toMillis(Metrics.LATENCY_WINDOW_SECONDS)

    private var lastChangeAtMs = 0L

    init {
        println("Stepping rate limiter by $stepValue to $initial")
    }

    /**
     * Updates the rate limiter based on current metrics and returns the new rate limit.
     *
     * This method handles two operational phases:
     * 1. Step phase: gradually ramps up load to the initial target
     * 2. Optimization phase: dynamically adjusts based on latency measurements
     *
     * @return The updated rate limit value
     */
    @Synchronized
    fun execute(): Double {
        // Handle fresh start or when reset() was called
        if (isStepPhase) {
            return handleStepPhase()
        }

        // Skip optimization if we don't have enough metrics
        if (getTotalOperations() < MIN_OPERATIONS) {
            log.info("Not enough operations performed yet to optimize")
            return rateLimiter.rate
        }

        // Skip optimization until the metrics describe the rate we set last time
        if (!hasSettled()) {
            log.debug("Waiting for the latency window to catch up with the last change")
            return rateLimiter.rate
        }

        // Get current latency metrics and optimize if available
        return getCurrentAndMaxLatency()
            .map { (currentLatency, maxLatency) -> optimizeRateLimit(currentLatency, maxLatency) }
            .orElse(rateLimiter.rate)
    }

    /**
     * Handles the initial step phase where we gradually increase load
     */
    private fun handleStepPhase(): Double {
        log.info("Stepping rate limiter by $stepValue")
        val newValue = minOf(rateLimiter.rate + stepValue, initial)

        // Check if we've reached the initial target
        if (newValue >= initial) {
            log.info("Moving to optimization phase")
            isStepPhase = false
        }

        applyRate(newValue)
        log.info("New rate limiter value: ${rateLimiter.rate}")
        return rateLimiter.rate
    }

    /**
     * Optimizes the rate limit based on current latency measurements
     */
    private fun optimizeRateLimit(
        currentLatency: Double,
        maxLatency: Long,
    ): Double {
        val currentRate = rateLimiter.rate
        val newLimit = getNextValue(currentRate, getCurrentTotalThroughput(), currentLatency, maxLatency)

        // No change needed
        if (newLimit == currentRate) {
            log.info("Optimizer has nothing to do")
            return currentRate
        }

        log.info("Updating rate limiter from $currentRate to $newLimit")
        return applyRate(newLimit)
    }

    /**
     * Sets the rate limiter and records when, so the next decision waits for metrics that describe it.
     */
    private fun applyRate(newLimit: Double): Double {
        rateLimiter.rate = newLimit
        lastChangeAtMs = clock()
        return newLimit
    }

    /**
     * Reports whether a full latency window has passed since the last change.
     */
    private fun hasSettled(): Boolean = clock() - lastChangeAtMs >= settleMs

    /**
     * Format a double to specified decimal places
     */
    private fun Double.format(decimals: Int): String = "%.${decimals}f".format(this)

    /**
     * Counts every operation type, so a workload of any shape can be optimized.
     *
     * The optimizer needs a meaningful number of samples behind its percentiles before it acts.
     */
    fun getTotalOperations(): Long =
        metrics.mutations.count +
            metrics.selects.count +
            metrics.deletions.count +
            metrics.populate.count

    fun getCurrentTotalThroughput(): Double =
        metrics.getSelectThroughput() +
            metrics.getMutationThroughput() +
            metrics.getDeletionThroughput() +
            metrics.getPopulateThroughput()

    /**
     * Determines which latency metric is most critical and returns it with its target.
     *
     * This method intelligently selects between read, write, and populate latencies
     * based on the current test phase and latency ratio to target.
     *
     * @return An Optional containing the current latency and its target maximum value
     */
    fun getCurrentAndMaxLatency(): Optional<Pair<Double, Long>> {
        // Case: No latency targets specified
        if (maxWriteLatency == null && maxReadLatency == null) {
            log.debug("No latency targets specified")
            return Optional.empty()
        }

        // Case: In populate phase
        if (isInPopulatePhase()) {
            return getPopulatePhaseLatency()
        }

        // Case: Only write latency target specified
        if (maxReadLatency == null && maxWriteLatency != null) {
            val writeLatency = getWriteLatency()
            log.info("Only write latency target specified, current: $writeLatency ms, max: $maxWriteLatency ms")
            return Optional.of(Pair(writeLatency, maxWriteLatency))
        }

        // Case: Only read latency target specified
        if (maxWriteLatency == null && maxReadLatency != null) {
            val readLatency = getReadLatency()
            log.info("Only read latency target specified, current: $readLatency ms, max: $maxReadLatency ms")
            return Optional.of(Pair(readLatency, maxReadLatency))
        }

        // Case: Both read and write latency targets specified - determine which is more critical
        return determineCriticalLatency()
    }

    /**
     * Checks if we're currently in the populate phase
     */
    private fun isInPopulatePhase(): Boolean =
        metrics.mutations.count == 0L &&
            metrics.selects.count == 0L &&
            metrics.deletions.count == 0L &&
            metrics.populate.count > 0L

    /**
     * Gets latency information during populate phase
     */
    private fun getPopulatePhaseLatency(): Optional<Pair<Double, Long>> {
        if (maxWriteLatency != null) {
            val populateLatency = getPopulateLatency()
            log.info("In populate phase, using populate latency: $populateLatency ms, max: $maxWriteLatency ms")
            return Optional.of(Pair(populateLatency, maxWriteLatency))
        }
        return Optional.empty()
    }

    /**
     * Determines which latency (read or write) is most critical relative to its target
     */
    private fun determineCriticalLatency(): Optional<Pair<Double, Long>> {
        val readLatency = getReadLatency()
        val writeLatency = getWriteLatency()

        // Calculate how close each latency is to its limit as a ratio
        val readLatencyRatio = readLatency / maxReadLatency!!.toDouble()
        val writeLatencyRatio = writeLatency / maxWriteLatency!!.toDouble()

        // Return the latency that's closest to or exceeding its target
        return if (readLatencyRatio > writeLatencyRatio) {
            log.debug("Read latency more critical: ${readLatencyRatio.format(2)} of max vs write ${writeLatencyRatio.format(2)}")
            Optional.of(Pair(readLatency, maxReadLatency))
        } else {
            log.debug("Write latency more critical: ${writeLatencyRatio.format(2)} of max vs read ${readLatencyRatio.format(2)}")
            Optional.of(Pair(writeLatency, maxWriteLatency))
        }
    }

    /**
     * Calculates the rate limit to use next, based on current performance metrics.
     *
     * This implements an adaptive algorithm with three cases:
     * 1. If latency exceeds target: cut back to below the throughput actually being achieved
     * 2. If within 90% of target latency: maintain current throughput to avoid oscillation
     * 3. If well below target: increase throughput proportionally to available headroom
     *
     * @param currentRate The current rate limit value
     * @param currentThroughput The throughput actually being achieved (ops/sec)
     * @param currentLatency The current observed latency (in ms)
     * @param maxLatency The maximum acceptable latency (in ms)
     * @return The calculated new rate limit, or currentRate to leave it alone
     */
    fun getNextValue(
        currentRate: Double,
        currentThroughput: Double,
        currentLatency: Double,
        maxLatency: Long,
    ): Double {
        val latencyRatio = currentLatency / maxLatency.toDouble()

        // Case 1: Latency is too high - reduce throughput.
        //
        // The reduction applies to the throughput actually achieved, not to the nominal limit.
        // Under overload the client falls behind its own limit, so a limit that sits above the
        // achieved rate constrains nothing and cutting it changes nothing.
        if (latencyRatio > 1.0) {
            // A throughput tracker that has not warmed up yet reports zero.  Falling back to the
            // current rate stops that from slamming the limiter down to the floor.
            val basis = if (currentThroughput > 0.0) minOf(currentRate, currentThroughput) else currentRate
            val newLimit = (basis * REDUCTION_FACTOR).coerceAtLeast(minRate)
            log.info(
                "Latency exceeded target: ${currentLatency.format(2)}ms > ${maxLatency}ms, " +
                    "reducing from $currentRate to ${newLimit.format(1)} " +
                    "(achieved throughput ${currentThroughput.format(1)})",
            )
            return newLimit
        }

        // Case 2: Within the dead band below target - maintain current throughput
        if (latencyRatio > DEAD_BAND) {
            log.info("Latency approaching target (${(latencyRatio * 100).format(1)}% of max), maintaining throughput")
            return currentRate
        }

        // Case 3: Well below target - increase, but only a limit we are already using.
        val utilizationRatio = currentThroughput / currentRate
        if (utilizationRatio < MIN_UTILIZATION) {
            log.info(
                "Not increasing rate limiter, current utilization too low (${utilizationRatio.format(2)})" +
                    " - throughput: $currentThroughput, limit: $currentRate",
            )
            return currentRate
        }

        // The increase scales with the fraction of the latency budget still unused.  That fraction
        // is dimensionless, so the response does not change when the target does.
        val adjustmentFactor = 1.0 + MAX_INCREASE * (1.0 - latencyRatio)
        val newLimit = currentRate * adjustmentFactor
        log.info(
            "Latency (${currentLatency.format(2)}ms) well below target (${maxLatency}ms): " +
                "increasing throughput by ${((adjustmentFactor - 1) * 100).format(1)}% " +
                "from $currentRate to ${newLimit.format(1)}",
        )
        return newLimit
    }

    /**
     * Gets the current 99th percentile read latency in milliseconds
     */
    fun getReadLatency() = metrics.selects.snapshot.get99thPercentile() * durationFactor

    /**
     * Gets the current 99th percentile write latency in milliseconds
     */
    fun getWriteLatency() = metrics.mutations.snapshot.get99thPercentile() * durationFactor

    /**
     * Gets the current 99th percentile populate operation latency in milliseconds
     */
    fun getPopulateLatency() = metrics.populate.snapshot.get99thPercentile() * durationFactor

    /**
     * Resets the optimizer to its initial state, starting the step phase again.
     * This is typically called after a populate phase completes or when workload parameters change.
     */
    @Synchronized
    fun reset() {
        log.info("Resetting rate limiter optimizer to step phase, starting rate: $stepValue")
        isStepPhase = true
        applyRate(stepValue)
    }
}
