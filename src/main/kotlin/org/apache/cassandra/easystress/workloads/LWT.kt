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
import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import org.apache.cassandra.easystress.PartitionKey
import org.apache.cassandra.easystress.StressContext
import java.util.concurrent.ConcurrentHashMap

class LWT : IStressWorkload {
    lateinit var insert: PreparedStatement
    lateinit var update: PreparedStatement
    lateinit var select: PreparedStatement
    lateinit var delete: PreparedStatement
    lateinit var deletePartition: PreparedStatement

    override fun schema(): List<String> = arrayListOf("""CREATE TABLE IF NOT EXISTS lwt (id text primary key, value int) """)

    override fun prepare(session: CqlSession) {
        insert = session.prepare("INSERT INTO lwt (id, value) VALUES (?, ?) IF NOT EXISTS")
        update = session.prepare("UPDATE lwt SET value = ? WHERE id = ? IF value = ?")
        select = session.prepare("SELECT * from lwt WHERE id = ?")
        delete = session.prepare("DELETE from lwt WHERE id = ? IF value = ?")
        deletePartition = session.prepare("DELETE from lwt WHERE id = ? IF EXISTS")
    }

    override fun getRunner(context: StressContext): IStressRunner {
        data class CallbackPayload(
            val id: String,
            val value: Int,
        )

        return object : IStressRunner {
            // onSuccess writes here from the driver's threads while the runner thread reads it
            val state = ConcurrentHashMap<String, Int>()

            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                val currentValue = state[partitionKey.getText()]
                val newValue: Int

                val mutation =
                    if (currentValue != null) {
                        newValue = currentValue + 1
                        update
                            .bind()
                            .setInt(0, newValue)
                            .setString(1, partitionKey.getText())
                            .setInt(2, currentValue)
                    } else {
                        newValue = 0
                        insert
                            .bind()
                            .setString(0, partitionKey.getText())
                            .setInt(1, newValue)
                    }
                val payload = CallbackPayload(partitionKey.getText(), newValue)
                return Operation.Mutation(mutation, payload)
            }

            override fun getNextSelect(partitionKey: PartitionKey): Operation =
                Operation.SelectStatement(
                    select
                        .bind()
                        .setString(0, partitionKey.getText()),
                )

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                val currentValue = state[partitionKey.getText()]

                val deletion =
                    if (currentValue != null) {
                        delete
                            .bind()
                            .setString(0, partitionKey.getText())
                            .setInt(1, currentValue)
                    } else {
                        deletePartition
                            .bind()
                            .setString(0, partitionKey.getText())
                    }
                return Operation.Deletion(deletion)
            }

            override fun onSuccess(
                op: Operation.Mutation,
                result: AsyncResultSet?,
            ) {
                val payload = op.callbackPayload!! as CallbackPayload
                state[payload.id] = payload.value
            }
        }
    }
}
