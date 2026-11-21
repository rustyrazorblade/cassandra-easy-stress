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
import com.google.common.base.Throwables
import org.apache.cassandra.easystress.workloads.IStressRunner
import org.apache.cassandra.easystress.workloads.Operation
import org.apache.logging.log4j.kotlin.logger
import java.util.concurrent.TimeUnit
import java.util.function.BiConsumer

/**
 * Callback after a mutation or select
 * This was moved out of the inline ProfileRunner to reduce clutter
 */
class OperationCallback(
    val context: StressContext,
    val runner: IStressRunner,
    val op: Operation,
    val startNanos: Long,
) : BiConsumer<AsyncResultSet?, Throwable?> {
    companion object {
        val log = logger()
    }

    override fun accept(
        result: AsyncResultSet?,
        t: Throwable?,
    ) {
        if (t != null) {
            context.metrics.errors.mark()
            context.collect(op, Either.Right(Throwables.getRootCause(t)), startNanos, System.nanoTime())
            log.error { t }
            return
        }

        val endNanos = System.nanoTime()
        context.timer(op).update(endNanos - op.createdAtNanos, TimeUnit.NANOSECONDS)
        // TODO (visibility): include details about paging?
        context.collect(op, Either.Left(result!!), startNanos, endNanos)

        // do the callback for mutations
        // might extend this to select, but I can't see a reason for it now
        when (op) {
            is Operation.Mutation -> {
                runner.onSuccess(op, result)
            }
            is Operation.DDL -> {
                runner.onSuccess(op, result)
            }
            is Operation.Stop -> {
                throw OperationStopException()
            }
            else -> {
                // ignore
            }
        }
    }
}
