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
package org.apache.cassandra.easystress.generators.functions

import org.apache.cassandra.easystress.converters.HumanReadableConverter
import org.apache.cassandra.easystress.generators.FieldGenerator
import org.apache.cassandra.easystress.generators.Function
import org.apache.commons.text.RandomStringGenerator
import java.util.concurrent.ThreadLocalRandom

@Function(
    name = "random",
    description = "Random numbers.",
)
class Random : FieldGenerator {
    var min = 0L
    var max = 100000L

    private val stringGenerator = RandomStringGenerator.Builder().withinRange(65, 90).build()

    override fun setParameters(params: List<String>) {
        min = HumanReadableConverter().convert(params[0])
        max = HumanReadableConverter().convert(params[1])
    }

    override fun getInt(): Int {
        if (min > Int.MAX_VALUE || max > Int.MAX_VALUE) {
            throw Exception("Int larger than Int.MAX_VALUE requested, use a long instead")
        }

        return ThreadLocalRandom.current().nextInt(min.toInt(), max.toInt())
    }

    override fun getText(): String {
        val length = ThreadLocalRandom.current().nextInt(min.toInt(), max.toInt())

        return stringGenerator.generate(length)
    }

    companion object {
        fun create(
            min: Long,
            max: Long,
        ) = Random()
            .apply {
                this.min = min
                this.max = max
            }
    }

    override fun getDescription() =
        """
        Completely random data with even distribution.
        """.trimIndent()
}
