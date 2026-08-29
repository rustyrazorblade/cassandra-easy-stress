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
import com.codahale.metrics.ScheduledReporter
import com.codahale.metrics.SlidingTimeWindowArrayReservoir
import com.codahale.metrics.Timer
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.dropwizard.DropwizardExports
import io.prometheus.client.exporter.HTTPServer
import java.util.Optional
import java.util.concurrent.TimeUnit

class Metrics(
    val metricRegistry: MetricRegistry,
    val reporters: List<ScheduledReporter>,
    httpPort: Int,
) {
    val server: Optional<HTTPServer>

    fun startReporting() {
        for (reporter in reporters) {
            reporter.start(3, TimeUnit.SECONDS)
        }
    }

    fun shutdown() {
        server.map { it.close() }

        for (reporter in reporters) {
            reporter.stop()
        }
        selectThroughputTracker.stop()
        mutationThroughputTracker.stop()
        deletionThroughputTracker.stop()
        populateThroughputTracker.stop()
    }

    fun resetErrors() {
        metricRegistry.remove("errors")
        errors = metricRegistry.meter("errors")
    }

    init {
        server =
            if (httpPort > 0) {
                CollectorRegistry.defaultRegistry.register(DropwizardExports(metricRegistry))
                Optional.of(HTTPServer(httpPort))
            } else {
                println("Not setting up prometheus endpoint.")
                Optional.empty()
            }
    }

    var errors = metricRegistry.meter("errors")
    val mutations = shortWindowTimer("mutations")
    val selects = shortWindowTimer("selects")
    val deletions = shortWindowTimer("deletions")

    val populate = shortWindowTimer("populateMutations")

    // Throughput trackers for metrics
    val selectThroughputTracker = getTracker { selects.count }.start()
    val mutationThroughputTracker = getTracker { mutations.count }.start()
    val deletionThroughputTracker = getTracker { deletions.count }.start()
    val populateThroughputTracker = getTracker { populate.count }.start()

    /**
     * Creates a timer whose percentiles cover only the recent past.
     *
     * The Dropwizard default is an ExponentiallyDecayingReservoir, which still weights samples
     * from about five minutes ago.  The rate limiter optimizer reads these percentiles every few
     * seconds, so that default makes it react to a signal it has already acted on, and the console
     * report shows a p99 that lags the run.  A short sliding window fixes both.
     */
    private fun shortWindowTimer(name: String) =
        metricRegistry.timer(name) {
            Timer(SlidingTimeWindowArrayReservoir(LATENCY_WINDOW_SECONDS, TimeUnit.SECONDS))
        }

    /**
     * We track throughput using separate structures than Dropwizard
     */
    fun resetThroughputTrackers() {
        selectThroughputTracker.reset()
        mutationThroughputTracker.reset()
        deletionThroughputTracker.reset()
        populateThroughputTracker.reset()
    }

    fun getTracker(countSupplier: () -> Long): ThroughputTracker =
        ThroughputTracker(
            windowSize = 10,
            countSupplier = countSupplier,
        )

    fun getSelectThroughput() = selectThroughputTracker.getCurrentThroughput()

    fun getMutationThroughput() = mutationThroughputTracker.getCurrentThroughput()

    fun getDeletionThroughput() = deletionThroughputTracker.getCurrentThroughput()

    fun getPopulateThroughput() = populateThroughputTracker.getCurrentThroughput()

    companion object {
        /**
         * The window latency percentiles are calculated over.  This must stay short enough that the
         * rate limiter optimizer sees the effect of its last adjustment before it makes the next one.
         */
        const val LATENCY_WINDOW_SECONDS = 10L
    }
}
