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

import com.codahale.metrics.Timer
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.google.common.util.concurrent.RateLimiter
import org.apache.cassandra.easystress.collector.Collector
import org.apache.cassandra.easystress.commands.Run
import org.apache.cassandra.easystress.generators.Registry
import org.apache.cassandra.easystress.workloads.Operation

data class StressContext(
    val session: CqlSession,
    val mainArguments: Run,
    val thread: Int,
    val metrics: Metrics,
    val registry: Registry,
    val rateLimiter: RateLimiter?,
    val collector: Collector,
) {
    fun collect(
        op: Operation,
        result: Either<AsyncResultSet, Throwable>,
        startNanos: Long,
        endNanos: Long,
    ) = collector.collect(this, op, result, startNanos, endNanos)

    // the populate phase records its own timer, see PopulateLoop
    fun timer(op: Operation): Timer =
        when (op) {
            is Operation.SelectStatement -> metrics.selects
            is Operation.Mutation -> metrics.mutations
            is Operation.Deletion -> metrics.deletions
            is Operation.Stop -> throw OperationStopException()
            // maybe this should be under DDL, it's a weird case.
            is Operation.DDL -> metrics.mutations
        }
}

data class Context(
    val session: CqlSession,
    val mainArguments: Run,
    val metrics: Metrics,
    val registry: Registry,
    val rateLimiter: RateLimiter?,
    val collector: Collector,
) {
    fun stress(thread: Int): StressContext = StressContext(session, mainArguments, thread, metrics, registry, rateLimiter, collector)
}
