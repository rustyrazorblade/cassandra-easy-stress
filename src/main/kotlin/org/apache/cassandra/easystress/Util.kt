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

import org.apache.commons.text.RandomStringGenerator
import java.text.DecimalFormat

/*
Will be used later when adding configuration
 */
typealias Range<T> = Pair<T, T>

private val randomStringGenerator = RandomStringGenerator.Builder().withinRange(65, 90).build()

/*
Throwaway - will need to be thought out for
 */
fun randomString(length: Int): String = randomStringGenerator.generate(length)

fun round(num: Double): Double = DecimalFormat("##.##").format(num).toDouble()
