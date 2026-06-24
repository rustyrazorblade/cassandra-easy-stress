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

import org.apache.cassandra.easystress.workloads.IStressRunner
import org.apache.cassandra.easystress.workloads.Operation
import org.apache.logging.log4j.kotlin.logger
import java.time.LocalDateTime
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadLocalRandom
import kotlin.concurrent.thread

/**
 * Request adds ability to set a proper rate limiter and addresses
 * the issue of coordinated omission.
 */
class RequestQueue(
    private val partitionKeyGenerator: PartitionKeyGenerator,
    context: StressContext,
    totalValues: Long,
    duration: Long,
    runner: IStressRunner,
    readRate: Double,
    deleteRate: Double,
    val populatePhase: Boolean = false,
) {
    val queue = ArrayBlockingQueue<Operation>(context.mainArguments.queueDepth.toInt(), true)
    var generatorThread: Thread

    companion object {
        val log = logger()
        private val QueueFullException = RuntimeException("Request queue is full, operation dropped")
    }

    init {

        generatorThread =
            thread(start = false) {
                // hack to ensure we don't start before the process is ready

                val desiredEndTime = LocalDateTime.now().plusSeconds(duration)
                var executed = 0L
                log.info("populate=$populatePhase total values: $totalValues, duration: $duration")

                for (key in partitionKeyGenerator.generateKey(totalValues, context.mainArguments.partitionCount)) {
                    if (duration > 0 && desiredEndTime.isBefore(LocalDateTime.now())) {
                        log.info("Reached duration, ending")
                        break
                    }

                    if (totalValues > 0 && executed == totalValues) {
                        log.info("Reached total values $totalValues")
                        break
                    }

                    // check if we hit our limit
                    // get next thing from the profile
                    // thing could be a statement, or it could be a failure command
                    // certain profiles will want to deterministically inject failures
                    // others can be randomly injected by the runner
                    // I should be able to just tell the runner to inject gossip failures in any test
                    // without having to write that code in the profile

                    val nextOp = ThreadLocalRandom.current().nextInt(0, 100)

                    context.rateLimiter?.run {
                        acquire(1)
                    }

                    // we only do the mutations in non-populate run
                    val op =
                        if (!populatePhase && readRate * 100 > nextOp) {
                            runner.getNextSelect(key)
                        } else if (deleteRate > 0.0 && (readRate * 100) + (deleteRate * 100) > nextOp) {
                            // we might be in a populate phase but only if the user specifically requested it
                            runner.getNextDelete(key)
                        } else if (populatePhase) {
                            // at this point we're either populating or we're in a normal run
                            runner.getNextPopulate(key)
                        } else {
                            runner.getNextMutation(key)
                        }

                    if (!queue.offer(op)) {
                        context.metrics.errors.mark()
                        val now = System.nanoTime()
                        context.collect(op, Either.Right(QueueFullException), now, now)
                    }
                    executed++
                }

                // wait for the queue to drain

                log.info("Finished queuing requests, waiting for queue to empty. $executed executed")
                Thread.sleep(1000)
                while (queue.size > 0) {
                    Thread.sleep(1000)
                }
                queue.add(Operation.Stop())
            }
    }

    fun getNextOperation() =
        sequence<Operation> {
            while (generatorThread.isAlive) {
                when (val tmp = queue.take()) {
                    is Operation.Stop -> break
                    else -> yield(tmp)
                }
            }
        }

    fun start() {
        generatorThread.start()
    }
}
