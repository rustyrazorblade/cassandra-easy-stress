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
package org.apache.cassandra.easystress.integration

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy
import org.apache.cassandra.easystress.PopulateRetryPolicy
import org.apache.cassandra.easystress.commands.Run
import org.apache.cassandra.easystress.converters.PopulateRowsConverter
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * The populate phase must write every row it is asked for.  Each test reads the table back
 * and checks the exact row count.
 */
class PopulateTest : CassandraTestBase() {
    private fun run(
        workload: String,
        configure: Run.() -> Unit,
    ) {
        Run("placeholder")
            .apply {
                this.workload = workload
                host = ip
                cqlPort = port
                dc = localDc
                dropKeyspace = true
                iterations = 10
                readRate = 0.0
                useOptimizer = false
                prometheusPort = 0
                replication = "{'class': 'SimpleStrategy', 'replication_factor':1 }"
                configure()
            }.execute()
    }

    private fun count(table: String): Long = connection.execute("SELECT count(*) FROM cassandra_easy_stress.$table").one()!!.getLong(0)

    /**
     * Populate only retries its writes if the session it actually runs on resolves the populate
     * profile to PopulateRetryPolicy.  The main phase must keep the driver's default.
     */
    @Test
    fun sessionUsesPopulateRetryPolicyOnlyForThePopulateProfile() {
        val run =
            Run("placeholder").apply {
                host = ip
                cqlPort = port
                dc = localDc
            }

        run.session.use { session ->
            val policies = session.context.retryPolicies

            assertThat(policies[PopulateRetryPolicy.POPULATE_PROFILE]).isInstanceOf(PopulateRetryPolicy::class.java)
            assertThat(policies[DriverExecutionProfile.DEFAULT_NAME]).isInstanceOf(DefaultRetryPolicy::class.java)
        }
    }

    @Test
    fun populateAllWritesEveryPartitionOfEveryThread() {
        // iterations = 10 is enough for the main phase, but it writes to keys populate already wrote
        run("KeyValue") {
            populate = PopulateRowsConverter.ALL
            partitionCount = 500
            threads = 2
            populateConcurrency = 8
        }

        assertThat(count("keyvalue")).isEqualTo(1000)
    }

    @Test
    fun batchesWriteEveryRowIntoItsPartition() {
        run("BasicTimeSeries") {
            populate = PopulateRowsConverter.ALL
            partitionCount = 20
            populateBatch = 5
            iterations = 2
        }

        // the main phase adds at most 2 rows of its own
        assertThat(count("sensor_data")).isBetween(100L, 102L)
        val partitions = connection.execute("SELECT DISTINCT sensor_id FROM cassandra_easy_stress.sensor_data").all()
        assertThat(partitions).hasSize(20)
    }

    @Test
    fun counterBatchesApplyEveryIncrement() {
        run("CountersWide") {
            populate = 400
            partitionCount = 10
            populateBatch = 4
            iterations = 2
        }

        val total =
            connection
                .execute("SELECT value FROM cassandra_easy_stress.counter_wide")
                .all()
                .sumOf { it.getLong("value") }
        // every populate write is one increment, and the main phase adds at most 2
        assertThat(total).isBetween(400L, 402L)
    }

    @Test
    fun lockingPopulatesTheKeysItsMainPhaseUses() {
        run("Locking") {
            partitionCount = 50
            threads = 2
        }

        assertThat(count("lwtupdates")).isEqualTo(100)
        val row = connection.execute("SELECT item_id FROM cassandra_easy_stress.lwtupdates WHERE item_id = '001.1.0'").one()
        assertThat(row).isNotNull()
    }

    @Test
    fun populateWritesNoTombstonesByDefaultEvenWithAMainPhaseDeleteRate() {
        run("KeyValue") {
            populate = 300
            partitionCount = 300
            deleteRate = 0.5
            iterations = 2
        }

        // with sequence keys and no populate deletes, all 300 rows are present, and the main phase deletes at most 2
        assertThat(count("keyvalue")).isGreaterThanOrEqualTo(298L)
    }

    /**
     * Locking's populate option counts partitions.  Each visit writes a batch, so a batch must not
     * shrink the number of partitions populated.
     */
    @Test
    fun aWorkloadPartitionCountStillCoversEveryPartitionWithABatch() {
        run("Locking") {
            partitionCount = 200
            populateBatch = 5
        }

        assertThat(count("lwtupdates")).isEqualTo(200)
    }

    /**
     * RangeScan can't write, so every populate thread throws.  The run has to fail instead of
     * starting the main phase on a data set populate never wrote.
     */
    @Test
    fun aPopulateThreadThatThrowsFailsTheRun() {
        assertThatThrownBy {
            run("RangeScan") {
                populate = 10
                threads = 2
            }
        }.isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("Populate failed on 2 of 2 threads")
    }

    /**
     * LWT records each value it writes in its runner.  Populate writes 0 to every key; if the main
     * phase shares that runner it updates each key to 1, but with a runner of its own it sends
     * INSERT ... IF NOT EXISTS, which changes nothing.
     */
    @Test
    fun theMainPhaseSeesTheStatePopulateLeftInTheRunner() {
        run("LWT") {
            populate = PopulateRowsConverter.ALL
            partitionCount = 50
            partitionKeyGenerator = "sequence"
            iterations = 50
        }

        val total =
            connection
                .execute("SELECT value FROM cassandra_easy_stress.lwt")
                .all()
                .sumOf { it.getInt("value") }
        assertThat(total).isEqualTo(50)
    }
}
