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
package com.rustyrazorblade.easycassstress

import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.rustyrazorblade.easycassstress.workloads.IStressRunner
import com.rustyrazorblade.easycassstress.workloads.Operation
import org.apache.logging.log4j.kotlin.logger
import java.util.function.BiConsumer

/**
 * Callback after a mutation or select
 * This was moved out of the inline ProfileRunner to make populate mode easier
 * as well as reduce clutter
 */
class OperationCallback(
    val context: StressContext,
    val runner: IStressRunner,
    val op: Operation,
    val paginate: Boolean = false,
    val writeHdr: Boolean = true,
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
            log.error { t }
            return
        }

        // Handle pagination in driver v4
        if (paginate && result != null) {
            // Fetch next page - this could be made async but we'll keep it simple for now
            while (result.hasMorePages()) {
                result.fetchNextPage()
            }
        }

        val time = op.startTime.stop()

        // we log to the HDR histogram and do the callback for mutations
        // might extend this to select, but I can't see a reason for it now
        when (op) {
            is Operation.Mutation -> {
                if (writeHdr) {
                    context.metrics.mutationHistogram.recordValue(time)
                }
                runner.onSuccess(op, result)
            }

            is Operation.Deletion -> {
                if (writeHdr) {
                    context.metrics.deleteHistogram.recordValue(time)
                }
            }

            is Operation.SelectStatement -> {
                if (writeHdr) {
                    context.metrics.selectHistogram.recordValue(time)
                }
            }
            is Operation.DDL -> {
                if (writeHdr) {
                    context.metrics.mutationHistogram.recordValue(time)
                }
                runner.onSuccess(op, result)
            }
            is Operation.Stop -> {
                throw OperationStopException()
            }
        }
    }
}
