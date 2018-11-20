/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer;

/**
 * A callback interface that the user can implement to allow code to execute when the request is complete. This callback
 * will generally execute in the background I/O thread so it should be fast.
 * 一个回调接口，用户可以实现该接口，以便在请求完成时执行代码。这个回调通常会在后台IO线程中执行，所以应该很快。
 */
public interface Callback {

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     * 用户可以实现回调方法来提供请求完成的异步处理。当发送到服务器的记录已被确认时，将调用此方法。其中一个参数将是benon-null。
     * @param metadata The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *        occurred.发送记录的元数据(即分区和偏移量)。如果发生错误，则为空。
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     *                  在处理此记录时抛出的异常。如果没有发生错误，则为空。
     *                  Possible thrown exceptions include:
     *可能抛出的异常包括
     *                  Non-Retriable exceptions (fatal, the message will never be sent):
     *不可检索异常(致命的是，消息永远不会被发送)
     *                  InvalidTopicException   无效的话题异常
     *                  OffsetMetadataTooLargeException 偏移过大的元数据异常
     *                  RecordBatchTooLargeException    记录批量太大的异常
     *                  RecordTooLargeException     记录过大异常
     *                  UnknownServerException      未知的服务器异常
     *
     *                  Retriable exceptions (transient, may be covered by increasing #.retries):
     *可检索异常(瞬态异常，可以通过增加#.retries来解决)
     *                  CorruptRecordException  腐败记录异常
     *                  InvalidMetadataException    无效的元数据异常
     *                  NotEnoughReplicasAfterAppendException   追加异常后的副本不够
     *                  NotEnoughReplicasException  没有足够的副本异常
     *                  OffsetOutOfRangeException   偏移超出范围异常
     *                  TimeoutException
     *                  UnknownTopicOrPartitionException
     */
    public void onCompletion(RecordMetadata metadata, Exception exception);
}
