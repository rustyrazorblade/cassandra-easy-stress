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
package org.apache.cassandra.easystress.workloads

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import org.apache.cassandra.easystress.PartitionKey
import org.apache.cassandra.easystress.PopulateOption
import org.apache.cassandra.easystress.StressContext
import org.apache.cassandra.easystress.commands.Run
import org.apache.logging.log4j.kotlin.logger
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap

/**
 * Note: currently broken :(
 * Warning: this workload is under development and should not be used as a reference across multiple cassandra-easy-stress runs with
 * different versions of cassandra-easy-stress as the implementation may change!
 *
 * Load test for a case where we have a dataset that requires LWT for a status update type workload
 * This could be a lock on status or a state machine in the real world
 *
 * For this test, we'll use the following states
 *
 * 0: normal
 * 1: temporarily locked
 */
class Locking : IStressWorkload {
    lateinit var insert: PreparedStatement
    lateinit var update: PreparedStatement
    lateinit var select: PreparedStatement
    lateinit var delete: PreparedStatement

    var log = logger()

    override fun prepare(session: CqlSession) {
        insert = session.prepare("INSERT INTO lwtupdates (item_id, name, status) VALUES (?, ?, 0)")
        update = session.prepare("UPDATE lwtupdates set status = ? WHERE item_id = ? IF status = ?")
        select = session.prepare("SELECT * from lwtupdates where item_id = ?")
        delete = session.prepare("DELETE from lwtupdates where item_id = ? IF EXISTS")
    }

    override fun schema(): List<String> {
        val query =
            """
            CREATE TABLE IF NOT EXISTS lwtupdates (
                item_id text primary key,
                name text,
                status int
            )
            """.trimIndent()
        return listOf(query)
    }

    override fun getPopulateOption(args: Run): PopulateOption = PopulateOption.Custom(args.partitionCount)

    override fun getPopulatePartitionKeyGenerator(): Optional<String> = Optional.of("sequence")

    override fun getRunner(context: StressContext): IStressRunner {
        return object : IStressRunner {
            // this test can't do more than 2 billion partition keys

            val state: ConcurrentHashMap<String, Int> = ConcurrentHashMap(context.mainArguments.partitionCount.toInt())

            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                val currentState = state.getOrDefault(partitionKey.getText(), 0)

                val newState =
                    when (currentState) {
                        0 -> 1
                        else -> 0
                    }

                log.trace { "Updating ${partitionKey.getText()} to $newState" }

                val bound =
                    update
                        .bind()
                        .setInt(0, newState)
                        .setString(1, partitionKey.getText())
                        .setInt(2, newState)
                state[partitionKey.getText()] = newState
                return Operation.Mutation(bound)
            }

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                val bound =
                    select
                        .bind()
                        .setString(0, partitionKey.getText())
                return Operation.SelectStatement(bound)
            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                val bound =
                    delete
                        .bind()
                        .setString(0, partitionKey.getText())
                return Operation.Deletion(bound)
            }

            override fun getNextPopulate(partitionKey: PartitionKey): Operation {
                val bound =
                    insert
                        .bind()
                        .setString(0, partitionKey.getText())
                        .setString(1, "test")
                return Operation.Mutation(bound)
            }
        }
    }
}
