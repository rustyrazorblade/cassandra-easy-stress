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

sealed class PopulateOption {
    class Standard : PopulateOption()

    /**
     * The workload sets how many partitions populate visits, in place of --populate.  Each visit
     * writes --populate.batch rows.  Deletes come only from --populate.deletes, which defaults to 0,
     * so a workload needs no option to turn them off.
     */
    class Custom(
        val partitions: Long,
    ) : PopulateOption()
}
