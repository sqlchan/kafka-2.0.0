/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log

/**
 * Configuration parameters for the log cleaner  日志清理器的配置参数
 * 
 * @param numThreads The number of cleaner threads to run  要运行的清理线程数
 * @param dedupeBufferSize The total memory used for log deduplication  用于日志重复数据删除的总内存
 * @param dedupeBufferLoadFactor The maximum percent full for the deduplication buffer  重复数据删除缓冲区的最大满百分比
 * @param maxMessageSize The maximum size of a message that can appear in the log  日志中显示的消息的最大大小
 * @param maxIoBytesPerSecond The maximum read and write I/O that all cleaner threads are allowed to do  允许所有线程执行的最大读写IO
 * @param backOffMs The amount of time to wait before rechecking if no logs are eligible for cleaning  在重新检查没有日志符合清理条件之前等待的时间量
 * @param enableCleaner Allows completely disabling the log cleaner  允许完全禁用日志清洁器
 * @param hashAlgorithm The hash algorithm to use in key comparison.  在关键比较中使用的哈希算法。
 */
case class CleanerConfig(numThreads: Int = 1,
                         dedupeBufferSize: Long = 4*1024*1024L,
                         dedupeBufferLoadFactor: Double = 0.9d,
                         ioBufferSize: Int = 1024*1024,
                         maxMessageSize: Int = 32*1024*1024,
                         maxIoBytesPerSecond: Double = Double.MaxValue,
                         backOffMs: Long = 15 * 1000,
                         enableCleaner: Boolean = true,
                         hashAlgorithm: String = "MD5") {
}
