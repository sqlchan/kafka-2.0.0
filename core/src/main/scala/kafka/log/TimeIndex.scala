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

import java.io.File
import java.nio.ByteBuffer

import kafka.utils.CoreUtils._
import kafka.utils.Logging
import org.apache.kafka.common.errors.InvalidOffsetException
import org.apache.kafka.common.record.RecordBatch

/**
 * An index that maps from the timestamp to the logical offsets of the messages in a segment.
  * 从时间戳映射到段中消息的逻辑偏移量的索引。
 * This index might be sparse, i.e. it may not hold an entry for all the messages in the segment.
 *这个索引可能是稀疏的，即它可能不包含段中所有消息的条目。
 * The index is stored in a file that is preallocated to hold a fixed maximum amount of 12-byte time index entries.
  * 该索引存储在一个预先分配的文件中，以保存一个固定的最大12字节时间索引条目
 * The file format is a series of time index entries. The physical format is a 8 bytes timestamp and a 4 bytes "relative" offset used in the [[OffsetIndex]].
  * 文件格式是一系列时间索引条目。物理格式是[[OffsetIndex]]中使用的8字节时间戳和4字节相对偏移量。
  * A time index entry (TIMESTAMP, OFFSET) means that the biggest timestamp seen before OFFSET is TIMESTAMP. i.e.
  * 时间索引条目(时间戳、偏移量)表示在偏移量之前看到的最大时间戳是时间戳。
 *  Any message whose timestamp is greater than TIMESTAMP must come after OFFSET.
 *任何时间戳大于时间戳的消息都必须在偏移量之后。
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal storage format.
 *所有外部api都从相对偏移量转换为完全偏移量，因此该类的用户不与内部存储格式交互。
 * The timestamps in the same time index file are guaranteed to be monotonically increasing.
 *保证同一时间索引文件中的时间戳是单调递增的。
 * The index support timestamp lookup for a memory map of this file. The lookup is done using a binary search to find the offset of the message whose indexed timestamp is closest but smaller or equals to the target timestamp.
 * 索引支持此文件的内存映射的时间戳查找。查找使用二进制搜索来查找索引时间戳与目标时间戳最接近但小于或等于的消息的偏移量。
 * Time index files can be opened in two ways: either as an empty, mutable index that allows appends or an immutable read-only index file that has previously been populated.
  * 时间索引文件可以以两种方式打开:一种是空的、允许追加的可变索引，另一种是以前填充过的不可变的只读索引文件。
 *  The makeReadOnly method will turn a mutable file into an immutable one and truncate off any extra bytes.
  *  makeReadOnly方法将把可变文件转换为不可变文件，并截断任何额外字节。
 *  This is done when the index file is rolled over.  这是在对索引文件进行滚动时完成的。
 *
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 *不尝试对此文件的内容进行校验和, 在发生崩溃时重新生成它。
 */
// Avoid shadowing mutable file in AbstractIndex  避免在AbstractIndex中隐藏可变文件
class TimeIndex(_file: File, baseOffset: Long, maxIndexSize: Int = -1, writable: Boolean = true)
    extends AbstractIndex[Long, Long](_file, baseOffset, maxIndexSize, writable) with Logging {

  @volatile private var _lastEntry = lastEntryFromIndexFile    //从索引文件中读取最后一个条目。

  override def entrySize = 12

  // We override the full check to reserve the last time index entry slot for the on roll call.
  //我们重写完整检查以保留上一次滚动调用的最后一次索引输入槽。
  override def isFull: Boolean = entries >= maxEntries - 1

  private def timestamp(buffer: ByteBuffer, n: Int): Long = buffer.getLong(n * entrySize)

  private def relativeOffset(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * entrySize + 8)

  def lastEntry: TimestampOffset = _lastEntry

  /**
   * Read the last entry from the index file. This operation involves disk access.
    * 从索引文件中读取最后一个条目。此操作涉及磁盘访问。
   */
  private def lastEntryFromIndexFile: TimestampOffset = {
    inLock(lock) {
      _entries match {
        case 0 => TimestampOffset(RecordBatch.NO_TIMESTAMP, baseOffset)
        case s => parseEntry(mmap, s - 1).asInstanceOf[TimestampOffset]
      }
    }
  }

  /**
   * Get the nth timestamp mapping from the time index  从时间索引获取第n个时间戳映射
   * @param n The entry number in the time index
   * @return The timestamp/offset pair at that entry
   */
  def entry(n: Int): TimestampOffset = {
    maybeLock(lock) {
      if(n >= _entries)
        throw new IllegalArgumentException("Attempt to fetch the %dth entry from a time index of size %d.".format(n, _entries))
      val idx = mmap.duplicate
      TimestampOffset(timestamp(idx, n), relativeOffset(idx, n))
    }
  }

  override def parseEntry(buffer: ByteBuffer, n: Int): IndexEntry = {
    TimestampOffset(timestamp(buffer, n), baseOffset + relativeOffset(buffer, n))
  }

  /**
   * Attempt to append a time index entry to the time index.  尝试将时间索引项附加到时间索引。
   * The new entry is appended only if both the timestamp and offsets are greater than the last appended timestamp and  the last appended offset.
   *只有当时间戳和偏移量都大于最后一个被追加的时间戳和最后一个被追加的偏移量时，才会追加新条目。
   *
   * @param timestamp The timestamp of the new time index entry  新时间索引项的时间戳
   * @param offset The offset of the new time index entry  新时间索引项的偏移量
   * @param skipFullCheck To skip checking whether the segment is full or not. We only skip the check when the segment gets rolled or the segment is closed.
   *        跳过检查片段是否已满。我们只在段被滚动或段被关闭时跳过检查。
   */
  def maybeAppend(timestamp: Long, offset: Long, skipFullCheck: Boolean = false) {
    inLock(lock) {
      if (!skipFullCheck)
        require(!isFull, "Attempt to append to a full time index (size = " + _entries + ").")
      // We do not throw exception when the offset equals to the offset of last entry. That means we are trying to insert the same time index entry as the last entry.
      // 当偏移量等于上一个条目的偏移量时，我们不会抛出异常。这意味着我们试图插入与最后一个条目相同的索引条目。
      // If the timestamp index entry to be inserted is the same as the last entry, we simply ignore the insertion because that could happen in the following two scenarios:
      //如果要插入的时间戳索引条目与上一个条目相同，那么我们只需忽略插入，因为这可能发生在以下两个场景中
      // 1. A log segment is closed. 日志段已关闭。
      // 2. LogSegment.onBecomeInactiveSegment() is called when an active log segment is rolled. 在滚动活动日志段时调用LogSegment.onBecomeInactiveSegment()
      if (_entries != 0 && offset < lastEntry.offset)
        throw new InvalidOffsetException("Attempt to append an offset (%d) to slot %d no larger than the last offset appended (%d) to %s."
          .format(offset, _entries, lastEntry.offset, file.getAbsolutePath))
      if (_entries != 0 && timestamp < lastEntry.timestamp)
        throw new IllegalStateException("Attempt to append a timestamp (%d) to slot %d no larger than the last timestamp appended (%d) to %s."
            .format(timestamp, _entries, lastEntry.timestamp, file.getAbsolutePath))
      // We only append to the time index when the timestamp is greater than the last inserted timestamp.
      //我们只在时间戳大于最后插入的时间戳时追加时间索引。
      // If all the messages are in message format v0, the timestamp will always be NoTimestamp. In that case, the time index will be empty.
      // 如果所有消息都是消息格式v0，那么时间戳将始终是NoTimestamp。在这种情况下，时间索引将为空。
      if (timestamp > lastEntry.timestamp) {
        debug("Adding index entry %d => %d to %s.".format(timestamp, offset, file.getName))
        mmap.putLong(timestamp)
        mmap.putInt(relativeOffset(offset))
        _entries += 1
        _lastEntry = TimestampOffset(timestamp, offset)
        require(_entries * entrySize == mmap.position(), _entries + " entries but file position in index is " + mmap.position() + ".")
      }
    }
  }

  /**
   * Find the time index entry whose timestamp is less than or equal to the given timestamp.
    * 查找时间戳小于或等于给定时间戳的时间索引条目。
   * If the target timestamp is smaller than the least timestamp in the time index, (NoTimestamp, baseOffset) is returned.
   * 如果目标时间戳小于时间索引中的最小时间戳，则返回(NoTimestamp, baseOffset)。
   *  二分法查找索引
   * @param targetTimestamp The timestamp to look up.
   * @return The time index entry found.
   */
  def lookup(targetTimestamp: Long): TimestampOffset = {
    maybeLock(lock) {
      val idx = mmap.duplicate
      val slot = largestLowerBoundSlotFor(idx, targetTimestamp, IndexSearchType.KEY)
      if (slot == -1)
        TimestampOffset(RecordBatch.NO_TIMESTAMP, baseOffset)
      else {
        val entry = parseEntry(idx, slot).asInstanceOf[TimestampOffset]
        TimestampOffset(entry.timestamp, entry.offset)
      }
    }
  }

  override def truncate() = truncateToEntries(0)  // 截短

  /**
   * Remove all entries from the index which have an offset greater than or equal to the given offset.
    * 从索引中删除所有偏移量大于或等于给定偏移量的条目。
   * Truncating to an offset larger than the largest in the index has no effect.
    * 截断到比索引中最大的偏移量更大的偏移量没有影响。
    */
  override def truncateTo(offset: Long) {
    inLock(lock) {
      val idx = mmap.duplicate
      val slot = largestLowerBoundSlotFor(idx, offset, IndexSearchType.VALUE)

      /* There are 3 cases for choosing the new size
       * 1) if there is no entry in the index <= the offset, delete everything
       * 2) if there is an entry for this exact offset, delete it and everything larger than it
       * 3) if there is no entry for this offset, delete everything larger than the next smallest
       */
      val newEntries =
        if(slot < 0)
          0
        else if(relativeOffset(idx, slot) == offset - baseOffset)
          slot
        else
          slot + 1
      truncateToEntries(newEntries)
    }
  }

  override def resize(newSize: Int): Boolean = {
    inLock(lock) {
      if (super.resize(newSize)) {
        _lastEntry = lastEntryFromIndexFile
        true
      } else
        false
    }
  }

  /**
   * Truncates index to a known number of entries. 将索引截断到已知数量的条目。
   */
  private def truncateToEntries(entries: Int) {
    inLock(lock) {
      _entries = entries
      mmap.position(_entries * entrySize)
      _lastEntry = lastEntryFromIndexFile
    }
  }
  //健全检查
  override def sanityCheck() {
    val lastTimestamp = lastEntry.timestamp
    val lastOffset = lastEntry.offset
    if (_entries != 0 && lastTimestamp < timestamp(mmap, 0))
      throw new CorruptIndexException(s"Corrupt time index found, time index file (${file.getAbsolutePath}) has " +
        s"non-zero size but the last timestamp is $lastTimestamp which is less than the first timestamp " +
        s"${timestamp(mmap, 0)}")
    if (_entries != 0 && lastOffset < baseOffset)
      throw new CorruptIndexException(s"Corrupt time index found, time index file (${file.getAbsolutePath}) has " +
        s"non-zero size but the last offset is $lastOffset which is less than the first offset $baseOffset")
    if (length % entrySize != 0)
      throw new CorruptIndexException(s"Time index file ${file.getAbsolutePath} is corrupt, found $length bytes " +
        s"which is neither positive nor a multiple of $entrySize.")
  }
}
