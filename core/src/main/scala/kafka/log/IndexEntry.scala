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

import org.apache.kafka.common.requests.ListOffsetResponse

sealed trait IndexEntry {
  // We always use Long for both key and value to avoid boxing. 我们总是使用Long键和value来避免拳击
  def indexKey: Long
  def indexValue: Long
}

/**
 * The mapping between a logical log offset and the physical position in some log file of the beginning of the message set entry with the given offset.
 * 逻辑日志偏移量与给定偏移量的消息集条目开头部分日志文件中的物理位置之间的映射。
 */
case class OffsetPosition(offset: Long, position: Int) extends IndexEntry {
  override def indexKey = offset
  override def indexValue = position.toLong
}


/**时间戳与消息偏移之间的映射
  * 该条目意味着其时间戳大于该时间戳的任何消息必须在该偏移量处或之后。
 * The mapping between a timestamp to a message offset. The entry means that any message whose timestamp is greater
 * than that timestamp must be at or after that offset.
 * @param timestamp The max timestamp before the given offset.  给定偏移量之前的最大时间戳。
 * @param offset The message offset.
 */
case class TimestampOffset(timestamp: Long, offset: Long) extends IndexEntry {
  override def indexKey = timestamp
  override def indexValue = offset
}

object TimestampOffset {
  val Unknown = TimestampOffset(ListOffsetResponse.UNKNOWN_TIMESTAMP, ListOffsetResponse.UNKNOWN_OFFSET)
}
