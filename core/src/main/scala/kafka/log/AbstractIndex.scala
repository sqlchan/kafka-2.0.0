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

import java.io.{File, RandomAccessFile}
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.concurrent.locks.{Lock, ReentrantLock}

import kafka.common.IndexOffsetOverflowException
import kafka.log.IndexSearchType.IndexSearchEntity
import kafka.utils.CoreUtils.inLock
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.utils.{MappedByteBuffers, OperatingSystem, Utils}

import scala.math.ceil

/**
 * The abstract index class which holds entry format agnostic methods.
 *抽象的索引类, 它持有条目格式无关的方法
 * @param file The index file 索引文件
 * @param baseOffset the base offset of the segment that this index is corresponding to.该索引对应的段的基偏移量。
 * @param maxIndexSize The maximum index size in bytes. 以字节为单位的最大索引大小。
 */
abstract class AbstractIndex[K, V](@volatile var file: File, val baseOffset: Long,
                                   val maxIndexSize: Int = -1, val writable: Boolean) extends Logging {

  // Length of the index file
  @volatile
  private var _length: Long = _

  protected def entrySize: Int

  protected val lock = new ReentrantLock

  @volatile
  protected var mmap: MappedByteBuffer = {
    val newlyCreated = file.createNewFile()
    val raf = if (writable) new RandomAccessFile(file, "rw") else new RandomAccessFile(file, "r")
    try {
      /* pre-allocate the file if necessary  必要时预分配文件 */
      if(newlyCreated) {
        if(maxIndexSize < entrySize)
          throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize)
        raf.setLength(roundDownToExactMultiple(maxIndexSize, entrySize))
      }

      /* memory-map the file  内存映射文件 */
      _length = raf.length()
      val idx = {
        if (writable)
          raf.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, _length)
        else
          raf.getChannel.map(FileChannel.MapMode.READ_ONLY, 0, _length)
      }
      /* set the position in the index for the next entry */
      if(newlyCreated)
        idx.position(0)
      else
        // if this is a pre-existing index, assume it is valid and set position to last entry
        idx.position(roundDownToExactMultiple(idx.limit(), entrySize))
      idx
    } finally {
      CoreUtils.swallow(raf.close(), this)
    }
  }

  /**
   * The maximum number of entries this index can hold
    * 此索引可以容纳的最大条目数
   */
  @volatile
  private[this] var _maxEntries = mmap.limit() / entrySize

  /** The number of entries in this index 这个索引中的条目数*/
  @volatile
  protected var _entries = mmap.position() / entrySize

  /**
   * True iff there are no more slots available in this index
   */
  def isFull: Boolean = _entries >= _maxEntries

  def maxEntries: Int = _maxEntries

  def entries: Int = _entries

  def length: Long = _length

  /**
   * Reset the size of the memory map and the underneath file. This is used in two kinds of cases: (1) in
   * trimToValidSize() which is called at closing the segment or new segment being rolled; (2) at
   * loading segments from disk or truncating back to an old segment where a new log segment became active;
   * we want to reset the index size to maximum index size to avoid rolling new segment.
   *重置内存映射和下面文件的大小。这在两种情况下使用:在关闭线段或新线段时调用trimToValidSize();
    * 从磁盘加载段或将段截断回旧段，在旧段中新的日志段开始活动
    * 我们希望将索引大小重置为最大索引大小，以避免滚动新段
   * @param newSize new size of the index file  索引文件的新大小
   * @return a boolean indicating whether the size of the memory map and the underneath file is changed or not.
    *         布尔值，指示内存映射和下面文件的大小是否改变
   */
  def resize(newSize: Int): Boolean = {
    inLock(lock) {
      val roundedNewSize = roundDownToExactMultiple(newSize, entrySize)

      if (_length == roundedNewSize) {
        false
      } else {
        val raf = new RandomAccessFile(file, "rw")
        try {
          val position = mmap.position()

          /* Windows won't let us modify the file length while the file is mmapped :-( */
          if (OperatingSystem.IS_WINDOWS)
            safeForceUnmap()
          raf.setLength(roundedNewSize)
          _length = roundedNewSize
          mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize)
          _maxEntries = mmap.limit() / entrySize
          mmap.position(position)
          true
        } finally {
          CoreUtils.swallow(raf.close(), this)
        }
      }
    }
  }

  /**
   * Rename the file that backs this offset index
   *重命名支持此偏移索引的文件
   * throws  IOException if rename fails
   */
  def renameTo(f: File) {
    try Utils.atomicMoveWithFallback(file.toPath, f.toPath)
    finally file = f
  }

  /**
   * Flush the data in the index to disk  将索引中的数据刷新到磁盘
   */
  def flush() {
    inLock(lock) {
      mmap.force()
    }
  }

  /**
   * Delete this index file.
   *删除此索引文件。
   * throws IOException if deletion fails due to an I/O error
   * @return `true` if the file was deleted by this method; `false` if the file could not be deleted because it did
   *         not exist
   */
  def deleteIfExists(): Boolean = {
    inLock(lock) {
      // On JVM, a memory mapping is typically unmapped by garbage collector.
      // However, in some cases it can pause application threads(STW) for a long moment reading metadata from a physical disk.
      // To prevent this, we forcefully cleanup memory mapping within proper execution which never affects API responsiveness.
      // See https://issues.apache.org/jira/browse/KAFKA-4614 for the details.
      safeForceUnmap()
    }
    Files.deleteIfExists(file.toPath)
  }

  /**
   * Trim this segment to fit just the valid entries, deleting all trailing unwritten bytes from
   * the file.
    * 修剪此段以适合于有效的条目，从文件中删除所有未写入的字节。
   */
  def trimToValidSize() {
    inLock(lock) {
      resize(entrySize * _entries)
    }
  }

  /**
   * The number of bytes actually used by this index 这个索引实际使用的字节数
   */
  def sizeInBytes = entrySize * _entries

  /** Close the index */
  def close() {
    trimToValidSize()
  }

  def closeHandler(): Unit = {
    inLock(lock) {
      safeForceUnmap()
    }
  }

  /**
   * Do a basic sanity check on this index to detect obvious problems
   *对此索引进行基本的健全检查, 以检测明显的问题
   * @throws CorruptIndexException if any problems are found
   */
  def sanityCheck(): Unit

  /**
   * Remove all the entries from the index.  从索引中删除所有条目
   */
  protected def truncate(): Unit

  /**
   * Remove all entries from the index which have an offset greater than or equal to the given offset.
    * 从索引中删除所有偏移量大于或等于给定偏移量的条目
   * Truncating to an offset larger than the largest in the index has no effect.
    * 截断到比索引中最大的偏移量更大的偏移量没有影响。
   */
  def truncateTo(offset: Long): Unit

  /**
   * Remove all the entries from the index and resize the index to the max index size.
    * 从索引中移除所有条目, 并将索引调整为最大索引大小。
   */
  def reset(): Unit = {
    truncate()
    resize(maxIndexSize)
  }

  /**
   * Get offset relative to base offset of this index
    * 获取该索引相对于基本偏移量的偏移量
   * @throws IndexOffsetOverflowException
   */
  def relativeOffset(offset: Long): Int = {
    val relativeOffset = toRelative(offset)
    if (relativeOffset.isEmpty)
      throw new IndexOffsetOverflowException(s"Integer overflow for offset: $offset (${file.getAbsoluteFile})")
    relativeOffset.get
  }

  /**
   * Check if a particular offset is valid to be appended to this index.
    * 检查特定的偏移量是否有效以附加到此索引。
   * @param offset The offset to check  要检查的偏移量
    *               如果这个偏移量是有效的，可以附加到这个索引
   * @return true if this offset is valid to be appended to this index; false otherwise
   */
  def canAppendOffset(offset: Long): Boolean = {
    toRelative(offset).isDefined
  }

  protected def safeForceUnmap(): Unit = {
    try forceUnmap()
    catch {
      case t: Throwable => error(s"Error unmapping index $file", t)
    }
  }

  /**
   * Forcefully free the buffer's mmap. 强制释放缓冲区的mmap。
   */
  protected[log] def forceUnmap() {
    try MappedByteBuffers.unmap(file.getAbsolutePath, mmap)
    finally mmap = null // Accessing unmapped mmap crashes JVM by SEGV so we null it out to be safe
    //通过SEGV访问未映射的mmap会导致JVM崩溃，所以为了安全起见，我们取消了它
  }

  /**
    * 只有在windows上运行时，才能在锁中执行给定的函数
    * Windows不会让我们在映射文件时调整其大小
   * Execute the given function in a lock only if we are running on windows. We do this
   * because Windows won't let us resize a file while it is mmapped. As a result we have to force unmap it
   * and this requires synchronizing reads.
    * 因此，我们必须强制取消映射，这需要同步读取。
   */
  protected def maybeLock[T](lock: Lock)(fun: => T): T = {
    if (OperatingSystem.IS_WINDOWS)
      lock.lock()
    try fun
    finally {
      if (OperatingSystem.IS_WINDOWS)
        lock.unlock()
    }
  }

  /**
   * To parse an entry in the index.
   *解析索引中的条目。
   * @param buffer the buffer of this memory mapped index. 这个内存映射索引的缓冲区
   * @param n the slot
   * @return the index entry stored in the given slot.
   */
  protected def parseEntry(buffer: ByteBuffer, n: Int): IndexEntry

  /**
   * Find the slot in which the largest entry less than or equal to the given target key or value is stored.
    * 找到存储小于或等于给定目标键或值的最大条目的位置
   * The comparison is made using the `IndexEntry.compareTo()` method.
   *使用“IndexEntry.compareTo()”方法进行比较。
   * @param idx The index buffer  索引缓冲区
   * @param target The index key to look for  要查找的索引键
   * @return The slot found or -1 if the least entry in the index is larger than the target key or the index is empty
   */
  protected def largestLowerBoundSlotFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchEntity): Int =
    indexSlotRangeFor(idx, target, searchEntity)._1

  /**
   * Find the smallest entry greater than or equal the target key or value. If none can be found, -1 is returned.
    * 找到大于或等于目标键或值的最小条目。如果找不到，则返回-1
   */
  protected def smallestUpperBoundSlotFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchEntity): Int =
    indexSlotRangeFor(idx, target, searchEntity)._2

  /**
   * Lookup lower and upper bounds for the given target.
    * 查找给定目标的下限和上限。
   */
  private def indexSlotRangeFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchEntity): (Int, Int) = {
    // check if the index is empty
    if(_entries == 0)
      return (-1, -1)

    // check if the target offset is smaller than the least offset
    if(compareIndexEntry(parseEntry(idx, 0), target, searchEntity) > 0)
      return (-1, 0)

    // binary search for the entry
    var lo = 0
    var hi = _entries - 1
    while(lo < hi) {
      val mid = ceil(hi/2.0 + lo/2.0).toInt
      val found = parseEntry(idx, mid)
      val compareResult = compareIndexEntry(found, target, searchEntity)
      if(compareResult > 0)
        hi = mid - 1
      else if(compareResult < 0)
        lo = mid
      else
        return (mid, mid)
    }

    (lo, if (lo == _entries - 1) -1 else lo + 1)
  }

  private def compareIndexEntry(indexEntry: IndexEntry, target: Long, searchEntity: IndexSearchEntity): Int = {
    searchEntity match {
      case IndexSearchType.KEY => indexEntry.indexKey.compareTo(target)
      case IndexSearchType.VALUE => indexEntry.indexValue.compareTo(target)
    }
  }

  /**
   * Round a number to the greatest exact multiple of the given factor less than the given number.
    * 将一个数精确到给定因数的最大倍数小于给定数
   * E.g. roundDownToExactMultiple(67, 8) == 64
   */
  private def roundDownToExactMultiple(number: Int, factor: Int) = factor * (number / factor)

  private def toRelative(offset: Long): Option[Int] = {
    val relativeOffset = offset - baseOffset
    if (relativeOffset < 0 || relativeOffset > Int.MaxValue)
      None
    else
      Some(relativeOffset.toInt)
  }

}

object IndexSearchType extends Enumeration {
  type IndexSearchEntity = Value
  val KEY, VALUE = Value
}
