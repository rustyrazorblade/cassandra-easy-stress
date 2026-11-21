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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class PartitionKeyGeneratorTest {
    @Test
    fun basicKeyGenerationTest() {
        val p = PartitionKeyGenerator.random("test")
        val tmp = p.generateKey(1000000)
        val pk = tmp.take(1).toList()[0]
        assertThat(pk.getText()).contains("test")
    }

    @Test
    fun sequenceTest() {
        val p = PartitionKeyGenerator.sequence("test")
        val result = p.generateKey(10, 10).first()
        assertThat(result.id).isEqualTo(0L)
    }

    @Test
    fun testRepeatingSequence() {
        val p = PartitionKeyGenerator.sequence("test")
        val data =
            p
                .generateKey(10, 2)
                .take(5)
                .toList()
                .map { it.id.toInt() }
        assertThat(data).isEqualTo(listOf(0, 1, 0, 1, 0))
    }

    @Test
    fun testNormal() {
        val p = PartitionKeyGenerator.normal("test")
        for (x in p.generateKey(1000, 1000)) {
        }
    }
}
