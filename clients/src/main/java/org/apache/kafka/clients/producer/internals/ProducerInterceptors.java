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
package org.apache.kafka.clients.producer.internals;


import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;

/**
 * A container that holds the list {@link org.apache.kafka.clients.producer.ProducerInterceptor}
 * and wraps calls to the chain of custom interceptors.
 */
public class ProducerInterceptors<K, V> implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(ProducerInterceptors.class);
    private final List<ProducerInterceptor<K, V>> interceptors;

    public ProducerInterceptors(List<ProducerInterceptor<K, V>> interceptors) {
        this.interceptors = interceptors;
    }

    /**
     * This is called when client sends the record to KafkaProducer, before key and value gets serialized.
     * The method calls {@link ProducerInterceptor#onSend(ProducerRecord)} method. ProducerRecord
     * returned from the first interceptor's onSend() is passed to the second interceptor onSend(), and so on in the
     * interceptor chain. The record returned from the last interceptor is returned from this method.
     *当客户端将记录发送到 kafkaproder 时, 在键和值被序列化之前, 将调用这一点。
     * 从第一个拦截器 onsend () 返回的生产记录被传递到拦截器链中的第二个拦截器 onsend (), 依此类推。
     * 从最后一个拦截器返回的记录将从此方法返回。
     * This method does not throw exceptions. Exceptions thrown by any of interceptor methods are caught and ignored.
     * If an interceptor in the middle of the chain, that normally modifies the record, throws an exception,
     * the next interceptor in the chain will be called with a record returned by the previous interceptor that did not
     * throw an exception.
     * 此方法不抛出异常。任何拦截器方法抛出的异常都会被捕获和忽略。
     * 如果链中间的拦截器(通常会修改记录)抛出异常，则链中的下一个拦截器将被调用，并返回上一个未抛出异常的拦截器返回的记录。
     *
     * @param record the record from client
     * @return producer record to send to topic/partition   生产者记录发送到topicpartition
     */
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        ProducerRecord<K, V> interceptRecord = record;
        for (ProducerInterceptor<K, V> interceptor : this.interceptors) {
            try {
                interceptRecord = interceptor.onSend(interceptRecord);
            } catch (Exception e) {
                // do not propagate interceptor exception, log and continue calling other interceptors  不传播拦截器异常，日志和继续调用其他拦截器
                // be careful not to throw exception from here  注意不要从这里抛出异常
                if (record != null)
                    log.warn("Error executing interceptor onSend callback for topic: {}, partition: {}", record.topic(), record.partition(), e);
                else
                    log.warn("Error executing interceptor onSend callback", e);
            }
        }
        return interceptRecord;
    }

    /**
     * This method is called when the record sent to the server has been acknowledged, or when sending the record fails before
     * it gets sent to the server. This method calls {@link ProducerInterceptor#onAcknowledgement(RecordMetadata, Exception)}
     * method for each interceptor.
     * 当发送到服务器的记录已被确认，或者在发送到服务器之前记录失败时，将调用此方法。
     * 这个方法为每个拦截器调用{@link ProducerInterceptor# on确认(RecordMetadata, Exception)}方法。
     *
     * This method does not throw exceptions. Exceptions thrown by any of interceptor methods are caught and ignored.
     * 此方法不抛出异常。任何拦截器方法抛出的异常都会被捕获和忽略。
     *
     * @param metadata The metadata for the record that was sent (i.e. the partition and offset).
     *                 发送记录的元数据(即分区和偏移量)。
     *                 If an error occurred, metadata will only contain valid topic and maybe partition.
     *                 如果发生错误，元数据将只包含有效的主题和分区。
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        for (ProducerInterceptor<K, V> interceptor : this.interceptors) {
            try {
                interceptor.onAcknowledgement(metadata, exception);
            } catch (Exception e) {
                // do not propagate interceptor exceptions, just log
                log.warn("Error executing interceptor onAcknowledgement callback", e);
            }
        }
    }

    /**
     * This method is called when sending the record fails in {@link ProducerInterceptor#onSend
     * (ProducerRecord)} method. This method calls {@link ProducerInterceptor#onAcknowledgement(RecordMetadata, Exception)}
     * method for each interceptor
     *
     * @param record The record from client
     * @param interceptTopicPartition  The topic/partition for the record if an error occurred
     *        after partition gets assigned; the topic part of interceptTopicPartition is the same as in record.
     * @param exception The exception thrown during processing of this record.
     */
    public void onSendError(ProducerRecord<K, V> record, TopicPartition interceptTopicPartition, Exception exception) {
        for (ProducerInterceptor<K, V> interceptor : this.interceptors) {
            try {
                if (record == null && interceptTopicPartition == null) {
                    interceptor.onAcknowledgement(null, exception);
                } else {
                    if (interceptTopicPartition == null) {
                        interceptTopicPartition = new TopicPartition(record.topic(),
                                record.partition() == null ? RecordMetadata.UNKNOWN_PARTITION : record.partition());
                    }
                    interceptor.onAcknowledgement(new RecordMetadata(interceptTopicPartition, -1, -1,
                                    RecordBatch.NO_TIMESTAMP, Long.valueOf(-1L), -1, -1), exception);
                }
            } catch (Exception e) {
                // do not propagate interceptor exceptions, just log
                log.warn("Error executing interceptor onAcknowledgement callback", e);
            }
        }
    }

    /**
     * Closes every interceptor in a container.
     */
    @Override
    public void close() {
        for (ProducerInterceptor<K, V> interceptor : this.interceptors) {
            try {
                interceptor.close();
            } catch (Exception e) {
                log.error("Failed to close producer interceptor ", e);
            }
        }
    }
}
