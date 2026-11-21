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

import org.apache.commons.math3.random.RandomDataGenerator
import java.util.concurrent.ThreadLocalRandom

/**
 * Accepts a function that generates numbers.
 * returns a partition key using a prefix
 * for any normal case there should be a compaction object function
 * that should create a generator that has a function with all the logic inside it
 */

class PartitionKeyGenerator(
    val genFunc: (max: Long) -> Long,
    val prefix: String,
) {
    /**
     *
     */
    companion object {
        /**
         *
         */
        fun random(prefix: String = "test"): PartitionKeyGenerator =
            PartitionKeyGenerator({ max -> ThreadLocalRandom.current().nextLong(0, max) }, prefix)

        /**
         * Walks 0 until max, then wraps, so it covers the same keys as [random].
         */
        fun sequence(prefix: String = "test"): PartitionKeyGenerator {
            var current = 0L
            return PartitionKeyGenerator(
                { max ->
                    if (current >= max) {
                        current = 0
                    }
                    current++
                },
                prefix,
            )
        }

        /**
         * Gaussian distribution
         */
        fun normal(prefix: String = "test"): PartitionKeyGenerator {
            val generator = RandomDataGenerator()
            return PartitionKeyGenerator({ max ->
                var result: Long
                while (true) {
                    val mid = (max / 2).toDouble()
                    result = generator.nextGaussian(mid, mid / 4.0).toLong()
                    if (result in 0..max) {
                        break
                    }
                }
                result
            }, prefix)
        }
    }

    fun generateKey(
        total: Long,
        maxId: Long = 100000,
    ) = sequence {
        var i: Long = 0
        while (true) {
            val tmp = genFunc(maxId)

            val result = PartitionKey(prefix, tmp)
            yield(result)
            i++

            if (i == total) {
                break
            }
        }
    }
}
