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
package org.apache.cassandra.easystress.converters

import com.beust.jcommander.IStringConverter
import com.beust.jcommander.ParameterException

/**
 * Accepts a human readable row count, or "all" (or -1, as MCP passes it) to populate every partition once.
 *
 * "all" can't be resolved here because it depends on --partitions and --populate.batch,
 * so it becomes [ALL] and Run resolves it.
 */
class PopulateRowsConverter : IStringConverter<Long> {
    companion object {
        const val ALL = -1L
        private val ROW_COUNT = Regex("""\d+[kKmMbB]?""")
    }

    // HumanReadableConverter drops the sign and turns anything it can't read into 0, so a typo
    // such as "al" would quietly populate nothing.  Only a count, "all" or -1 gets through.
    override fun convert(value: String?): Long {
        val text = value?.trim().orEmpty()
        return when {
            text.equals("all", ignoreCase = true) || text == ALL.toString() -> ALL
            ROW_COUNT.matches(text) -> HumanReadableConverter().convert(text)
            else -> throw ParameterException("--populate must be a row count such as 10000 or 10k, or 'all'; got '$value'")
        }
    }
}
