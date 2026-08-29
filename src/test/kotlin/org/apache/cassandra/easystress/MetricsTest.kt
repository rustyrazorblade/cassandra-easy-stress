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
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

class MetricsTest {
    /**
     * The rate limiter optimizer reads these percentiles every few seconds.  The Dropwizard default
     * reservoir weights samples from about five minutes ago, which makes the optimizer act on a
     * signal it has already responded to.
     *
     * The default reservoir also caps itself at 1028 samples.  A sliding time window keeps every
     * sample recorded inside the window, so the sample count tells the two apart.
     */
    @Test
    fun latencyPercentilesCoverOnlyTheRecentPast() {
        val metrics = Metrics(MetricRegistry(), emptyList(), 0)

        try {
            val timers = listOf(metrics.mutations, metrics.selects, metrics.deletions, metrics.populate)

            for (timer in timers) {
                repeat(2000) {
                    timer.update(1, TimeUnit.MILLISECONDS)
                }
                assertThat(timer.snapshot.size()).isEqualTo(2000)
            }
        } finally {
            metrics.shutdown()
        }
    }
}
