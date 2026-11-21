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
import com.datastax.oss.driver.api.core.cql.BoundStatement
import org.apache.cassandra.easystress.PartitionKey
import org.apache.cassandra.easystress.PopulateOption
import org.apache.cassandra.easystress.StressContext
import org.apache.cassandra.easystress.commands.Run
import org.apache.cassandra.easystress.generators.Field
import org.apache.cassandra.easystress.generators.FieldGenerator
import java.util.Optional

interface IStressRunner {
    fun getNextMutation(partitionKey: PartitionKey): Operation

    fun getNextSelect(partitionKey: PartitionKey): Operation

    fun getNextDelete(partitionKey: PartitionKey): Operation

    /**
     * Populate phase will typically just perform regular mutations.
     * However, certain workloads may need custom setup.
     * @see Locking
     **/
    fun getNextPopulate(partitionKey: PartitionKey): Operation = getNextMutation(partitionKey)

    /**
     * Callback after a query executes successfully.
     * Will be used for state tracking on things like LWTs as well as provides an avenue for future work
     * doing post-workload correctness checks
     */
    fun onSuccess(
        op: Operation.Mutation,
        result: AsyncResultSet?,
    ) { }

    fun onSuccess(
        op: Operation.DDL,
        result: AsyncResultSet?,
    ) { }
}

/**
 * Stress profile interface.  A stress profile defines the schema, prepared
 * statements, and queries that will be executed.  It should be fairly trivial
 * to imp
 */
interface IStressWorkload {
    /**
     * Handles any prepared statements that are needed
     * the class should track all prepared statements internally
     * and pass them on to the Runner
     */
    fun prepare(session: CqlSession)

    /**
     * returns a bunch of DDL statements
     * this can be any valid DDL such as
     * CREATE table, index, materialized view, etc
     * for most tests this is probably a single table
     * it's OK to put a clustering order in, but otherwise the schema
     * should not specify any other options here because they can all
     * but supplied on the command line.
     *
     * I may introduce a means of supplying default values, because
     * there are plenty of use cases where you would want a specific
     * compaction strategy most of the time (like a time series, or a cache)
     */
    fun schema(): List<String>

    /**
     * returns an instance of the stress runner for this particular class
     * This was done to allow a single instance of an IStress profile to be
     * generated, and passed to the ProfileRunner.
     * The issue is that the profile needs to generate a single schema
     * but then needs to create multiple stress runners
     * this allows the code to be a little cleaner
     */
    fun getRunner(context: StressContext): IStressRunner

    /**
     * returns a map of generators corresponding to the different fields
     * it's required to specify all fields that use a generator
     * some fields don't, like TimeUUID or the first partition key
     * This is optional, but encouraged
     *
     * A profile can technically do whatever it wants, no one is obligated to use the generator
     * Using this does give the flexibility of specifying a different generator however
     * In the case of text fields, this is VERY strongly encouraged to allow for more flexibility with the size
     * of the text payload
     */
    fun getFieldGenerators(): Map<Field, FieldGenerator> = mapOf()

    fun getDefaultReadRate(): Double = .01

    fun getPopulateOption(args: Run): PopulateOption = PopulateOption.Standard()

    /**
     * Name of the partition key generator populate uses, unless --populate.generator is given.
     * Returns a name rather than a generator so the runner can give it the per-thread key prefix.
     */
    fun getPopulatePartitionKeyGenerator(): Optional<String> = Optional.empty()
}

sealed class Operation(
    val bound: BoundStatement? = null,
    val statement: String? = null,
) {
    val createdAtNanos = System.nanoTime()
    val createdAtMillis = System.currentTimeMillis()

    class Mutation(
        bound: BoundStatement,
        val callbackPayload: Any? = null,
    ) : Operation(bound)

    class SelectStatement(
        bound: BoundStatement,
    ) : Operation(bound)

    class Deletion(
        bound: BoundStatement,
    ) : Operation(bound)

    class Stop : Operation(null)

    class DDL(
        statement: String,
    ) : Operation(null, statement = statement)
}
