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

import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.oss.driver.api.core.context.DriverContext
import com.datastax.oss.driver.api.core.retry.RetryDecision
import com.datastax.oss.driver.api.core.retry.RetryPolicy
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException
import com.datastax.oss.driver.api.core.servererrors.WriteType
import com.datastax.oss.driver.api.core.session.Request
import org.apache.logging.log4j.kotlin.logger

/**
 * Retry policy for the populate phase, attached to the [POPULATE_PROFILE] execution profile.
 *
 * The populate phase exists to load a known data set, so a write that fails is a row that is missing
 * from the test.  The driver's default policy retries almost nothing for a write.  This one retries
 * every failure until [MAX_RETRIES] is reached.  The driver only asks the policy about a write
 * timeout or an aborted request when the statement is idempotent, so populate marks every
 * statement idempotent.
 *
 * Retrying on the next node takes that node from the request's query plan, and the request fails
 * once the plan is empty, which on a three node cluster is after two retries.  So a timeout or an
 * unavailable, which the coordinator reports about its replicas, is retried on the same coordinator.
 * A failure of the coordinator itself goes to the next node: an aborted request, where the connection
 * failed, and an error response, such as the OverloadedException a node sends while it shuts down.
 * Ten retries on a node that is shutting down all fail.
 *
 * The driver gives no way to wait between attempts.  The populate in-flight limit bounds how much
 * extra load the retries can add.
 */
@Suppress("OVERRIDE_DEPRECATION", "UNUSED_PARAMETER")
class PopulateRetryPolicy(
    context: DriverContext?,
    profileName: String?,
) : RetryPolicy {
    companion object {
        const val POPULATE_PROFILE = "populate"
        const val MAX_RETRIES = 10
        val log = logger()
    }

    private fun decide(
        reason: String,
        retryCount: Int,
        retry: RetryDecision = RetryDecision.RETRY_SAME,
    ): RetryDecision =
        if (retryCount < MAX_RETRIES) {
            log.debug { "Retrying populate write after $reason, attempt ${retryCount + 1} of $MAX_RETRIES" }
            retry
        } else {
            RetryDecision.RETHROW
        }

    override fun onReadTimeout(
        request: Request,
        cl: ConsistencyLevel,
        blockFor: Int,
        received: Int,
        dataPresent: Boolean,
        retryCount: Int,
    ): RetryDecision = decide("read timeout", retryCount)

    override fun onWriteTimeout(
        request: Request,
        cl: ConsistencyLevel,
        writeType: WriteType,
        blockFor: Int,
        received: Int,
        retryCount: Int,
    ): RetryDecision = decide("write timeout", retryCount)

    override fun onUnavailable(
        request: Request,
        cl: ConsistencyLevel,
        required: Int,
        alive: Int,
        retryCount: Int,
    ): RetryDecision = decide("unavailable", retryCount)

    override fun onRequestAborted(
        request: Request,
        error: Throwable,
        retryCount: Int,
    ): RetryDecision = decide("aborted request", retryCount, RetryDecision.RETRY_NEXT)

    override fun onErrorResponse(
        request: Request,
        error: CoordinatorException,
        retryCount: Int,
    ): RetryDecision = decide(error.javaClass.simpleName, retryCount, RetryDecision.RETRY_NEXT)

    override fun close() {
    }
}
