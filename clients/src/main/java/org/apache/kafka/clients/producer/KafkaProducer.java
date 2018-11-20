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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.clients.producer.internals.ProducerMetrics;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.clients.producer.internals.TransactionalRequestResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.serialization.ExtendedSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.common.serialization.ExtendedSerializer.Wrapper.ensureExtended;

/**
 * A Kafka client that publishes records to the Kafka cluster.  Kafka客户端，向Kafka集群发布记录。
 * <P>
 * The producer is <i>thread safe</i> and sharing a single producer instance across threads will generally be faster than
 * having multiple instances.   生产者是线程安全的，跨线程共享一个生产者实例通常比拥有多个实例快。
 * <p>
 * Here is a simple example of using the producer to send records with strings containing sequential numbers as the key/value
 * pairs.   下面是一个简单的示例，使用生成器将包含序号的字符串作为键值对发送记录。
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("acks", "all");
 * props.put("retries", 0);
 * props.put("batch.size", 16384);
 * props.put("linger.ms", 1);
 * props.put("buffer.memory", 33554432);
 * props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 * props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 *
 * Producer<String, String> producer = new KafkaProducer<>(props);
 * for (int i = 0; i < 100; i++)
 *     producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
 *
 * producer.close();
 * }</pre>
 * <p>
 * The producer consists of a pool of buffer space that holds records that haven't yet been transmitted to the server
 * as well as a background I/O thread that is responsible for turning these records into requests and transmitting them
 * to the cluster. Failure to close the producer after use will leak these resources.
 * 生产者包含一个缓冲空间池，用于保存尚未传输到服务器的记录，
 * 以及一个后台IO线程，负责将这些记录转换为请求并将其传输到集群。使用后不关闭生产者将泄漏这些资源。
 * <p>
 * The {@link #send(ProducerRecord) send()} method is asynchronous. When called it adds the record to a buffer of pending record sends
 * and immediately returns. This allows the producer to batch together individual records for efficiency.
 * 方法是异步的。调用时，它将记录添加到挂起记录发送和立即返回的缓冲区中。这允许生产者将单个记录批量处理以提高效率。
 * <p>
 * The <code>acks</code> config controls the criteria under which requests are considered complete. The "all" setting
 * we have specified will result in blocking on the full commit of the record, the slowest but most durable setting.
 * acks配置控制的条件下，请求被认为是完整的。我们指定的all设置将导致阻塞记录的完整提交，这是最慢但最持久的设置。
 * <p>
 * If the request fails, the producer can automatically retry, though since we have specified <code>retries</code>
 * as 0 it won't. Enabling retries also opens up the possibility of duplicates (see the documentation on
 * <a href="http://kafka.apache.org/documentation.html#semantics">message delivery semantics</a> for details).
 * 如果请求失败, 生成器可以自动重试, 但由于我们已将重试指定为 0, 因此它不会重试。
 * 启用重试也可能会出现重复 (有关详细信息, 请参阅有关消息传递语义的文档)。
 * <p>
 * The producer maintains buffers of unsent records for each partition. These buffers are of a size specified by
 * the <code>batch.size</code> config. Making this larger can result in more batching, but requires more memory (since we will
 * generally have one of these buffers for each active partition).
 * 生成器维护每个分区的未发送记录的缓冲区。这些缓冲区的大小由批处理. size 配置指定。
 * 使其更大可能会导致更多的批处理, 但需要更多的内存 (因为我们通常为每个活动分区都有其中一个缓冲区)。
 * <p>
 * By default a buffer is available to send immediately even if there is additional unused space in the buffer. However if you
 * want to reduce the number of requests you can set <code>linger.ms</code> to something greater than 0. This will
 * instruct the producer to wait up to that number of milliseconds before sending a request in hope that more records will
 * arrive to fill up the same batch. This is analogous to Nagle's algorithm in TCP. For example, in the code snippet above,
 * likely all 100 records would be sent in a single request since we set our linger time to 1 millisecond.
 * 默认情况下, 即使缓冲区中有其他未使用的空间, 也可以立即发送缓冲区。
 * 但是, 如果要减少请求数, 可以将 linger. ms 设置为大于0的请求。这将指示生成器在发送请求之前等待最多该毫秒数, 希望会有更多的记录到达以填充同一批。
 * 这类似于 ndle 在 tcp 中的算法。
 * 例如, 在上面的代码段中, 可能所有100条记录都将在一个请求中发送, 因为我们将延迟时间设置为1毫秒。
 * However this setting
 * would add 1 millisecond of latency to our request waiting for more records to arrive if we didn't fill up the buffer. Note that
 * records that arrive close together in time will generally batch together even with <code>linger.ms=0</code> so under heavy load
 * batching will occur regardless of the linger configuration; however setting this to something larger than 0 can lead to fewer, more
 * efficient requests when not under maximal load at the cost of a small amount of latency.
 * 但是, 如果我们没有填满缓冲区, 此设置将为我们的请求添加1毫秒的延迟, 等待更多记录到达。
 * 请注意, 及时到达的记录通常会一起批处理, 即使是 ling尔. mss0.0, 因此在重载批处理下, 无论延迟配置如何, 都会发生;
 * 但是, 如果不在最大负载下, 以少量延迟为代价, 则将此设置为大于0的内容可能会导致更少、更高效的请求。
 * <p>
 * The <code>buffer.memory</code> controls the total amount of memory available to the producer for buffering. If records
 * are sent faster than they can be transmitted to the server then this buffer space will be exhausted. When the buffer space is
 * exhausted additional send calls will block. The threshold for time to block is determined by <code>max.block.ms</code> after which it throws
 * a TimeoutException.
 * 缓冲区. 内存控制生成器可用于缓冲的内存总量。如果记录的发送速度快于传输到服务器的速度, 则此缓冲区空间将耗尽。
 * 当缓冲区空间耗尽时, 将阻止额外的发送调用。要阻塞的时间的阈值由 max.block.ms 决定, 之后它将引发时间异常
 * <p>
 * The <code>key.serializer</code> and <code>value.serializer</code> instruct how to turn the key and value objects the user provides with
 * their <code>ProducerRecord</code> into bytes. You can use the included {@link org.apache.kafka.common.serialization.ByteArraySerializer} or
 * {@link org.apache.kafka.common.serialization.StringSerializer} for simple string or byte types.
 * <p>
 * From Kafka 0.11, the KafkaProducer supports two additional modes: the idempotent producer and the transactional producer.
 * The idempotent producer strengthens Kafka's delivery semantics from at least once to exactly once delivery. In particular
 * producer retries will no longer introduce duplicates. The transactional producer allows an application to send messages
 * to multiple partitions (and topics!) atomically.
 * 从卡夫卡 0.11, 卡夫卡制作者支持另外两种模式: 等价生产者和交易生产者。等价生成器至少一次到一次的传递加强了卡夫卡的交付语义。
 * 特别是, 生成器重试将不再引入重复项。事务生成器允许应用程序以原子方式将消息发送到多个分区 (和主题!)。
 * </p>
 * <p>
 * To enable idempotence, the <code>enable.idempotence</code> configuration must be set to true. If set, the
 * <code>retries</code> config will default to <code>Integer.MAX_VALUE</code> and the <code>acks</code> config will
 * default to <code>all</code>. There are no API changes for the idempotent producer, so existing applications will
 * not need to be modified to take advantage of this feature.
 * 若要启用等价性, 必须将启用性. 等价配置设置为 true。如果设置, 则 "选择重试配置" 将默认为 "输入"。
 * 对于等价生成器, 没有 api 更改, 因此不需要修改现有应用程序来利用此功能。
 * </p>
 * <p>
 * To take advantage of the idempotent producer, it is imperative to avoid application level re-sends since these cannot
 * be de-duplicated. As such, if an application enables idempotence, it is recommended to leave the <code>retries</code>
 * config unset, as it will be defaulted to <code>Integer.MAX_VALUE</code>.
 * 为了利用幂等生产者，必须避免应用程序级别的重新发送，因为它们不能被反复制。
 * 因此，如果应用程序启用了幂等性，建议将重试配置保留为未设置，因为默认值为整数。最大价值。
 * Additionally, if a {@link #send(ProducerRecord)}
 * returns an error even with infinite retries (for instance if the message expires in the buffer before being sent),
 * then it is recommended to shut down the producer and check the contents of the last produced message to ensure that
 * it is not duplicated. Finally, the producer can only guarantee idempotence for messages sent within a single session.
 * 此外，如果{@link #send(ProducerRecord)}返回一个错误，即使是无限次重试(例如，如果消息在发送之前在缓冲区中过期)，
 * 那么建议关闭生产者并检查最后生成的消息的内容，以确保它没有重复。
 * 最后，生产者只能保证在单个会话中发送的消息的幂等性
 * </p>
 * <p>To use the transactional producer and the attendant APIs, you must set the <code>transactional.id</code>
 * configuration property. If the <code>transactional.id</code> is set, idempotence is automatically enabled along with
 * the producer configs which idempotence depends on.
 * 要使用事务性生产者和附属api，您必须设置事务性。id配置属性。
 * 如果事务。id设置好了，幂等性就会随着生产者配置自动启用，而幂等性依赖于这些配置。
 * 此外，应该为事务中包含的主题进行持久性配置。
 * Further, topics which are included in transactions should be configured
 * for durability. In particular, the <code>replication.factor</code> should be at least <code>3</code>, and the
 * <code>min.insync.replicas</code> for these topics should be set to 2. Finally, in order for transactional guarantees
 * to be realized from end-to-end, the consumers must be configured to read only committed messages as well.
 * 特别是, 复制. 因子应至少为 3, 并且这些主题的 min.insync.replicas 应设置为2。
 * 最后, 为了从端到端实现事务性保证, 必须将使用者配置为也只读取提交的消息。
 * </p>
 * <p>
 * The purpose of the <code>transactional.id</code> is to enable transaction recovery across multiple sessions of a
 * single producer instance. It would typically be derived from the shard identifier in a partitioned, stateful, application.
 * 事务处理 id 的目的是在单个生成器实例的多个会话中实现事务恢复。它通常来自分区、有状态的应用程序中的分片标识符。
 * As such, it should be unique to each producer instance running within a partitioned application.
 * 因此，对于在分区应用程序中运行的每个生产者实例，它应该是唯一的。
 * </p>
 * <p>All the new transactional APIs are blocking and will throw exceptions on failure. The example
 *  * below illustrates how the new APIs are meant to be used. It is similar to the example above, except that all
 *  * 100 messages are part of a single transaction.
 *  所有新的事务性 api 都在阻塞, 并将在失败时引发异常。
 *  下面的示例说明了如何使用新的 api。它与上面的示例类似, 只是所有100条消息都是单个事务的一部分。
 * </p>
 * <p>
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("transactional.id", "my-transactional-id");
 * Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
 *
 * producer.initTransactions();
 *
 * try {
 *     producer.beginTransaction();
 *     for (int i = 0; i < 100; i++)
 *         producer.send(new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i)));
 *     producer.commitTransaction();
 * } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
 *     // We can't recover from these exceptions, so our only option is to close the producer and exit.
 *     producer.close();
 *     //我们无法从这些异常中恢复，所以我们唯一的选择是关闭生产者并退出。producer.close ()
 * } catch (KafkaException e) {
 *     // For all other exceptions, just abort the transaction and try again.
 *     producer.abortTransaction();
 *     //对于所有其他异常, 只需中止事务, 然后重试。生产者. aberttrade ();
 * }
 * producer.close();
 * } </pre>
 * </p>
 * <p>
 * As is hinted at in the example, there can be only one open transaction per producer. All messages sent between the
 * {@link #beginTransaction()} and {@link #commitTransaction()} calls will be part of a single transaction. When the
 * <code>transactional.id</code> is specified, all messages sent by the producer must be part of a transaction.
 * 如示例中所示，每个生产者只能有一个开放事务。在{@link #beginTransaction()}和
 * {@link #commitTransaction()}调用之间发送的所有消息都是单个事务的一部分。
 * 当事务。指定了id，生产者发送的所有消息都必须是事务的一部分。
 * </p>
 * <p>
 * The transactional producer uses exceptions to communicate error states. In particular, it is not required
 * to specify callbacks for <code>producer.send()</code> or to call <code>.get()</code> on the returned Future: a
 * <code>KafkaException</code> would be thrown if any of the
 * <code>producer.send()</code> or transactional calls hit an irrecoverable error during a transaction. See the {@link #send(ProducerRecord)}
 * documentation for more details about detecting errors from a transactional send.
 * 事务性生产者使用异常来通信错误状态。特别是，对于返回的将来，不需要为producer.send()或调用.get()来指定回调:
 * 如果任何producer.send()或事务调用在事务期间遇到不可恢复的错误，就会抛出KafkaException。
 * 有关从事务性发送检测错误的详细信息，请参阅{@link #send(ProducerRecord)}文档。
 * </p>
 * </p>By calling
 * <code>producer.abortTransaction()</code> upon receiving a <code>KafkaException</code> we can ensure that any
 * successful writes are marked as aborted, hence keeping the transactional guarantees.
 * 通过在接收到KafkaException后调用producer.abortTransaction()，我们可以确保任何成功的写都被标记为aborted，从而保持事务保证。
 * </p>
 * <p>
 * This client can communicate with brokers that are version 0.10.0 or newer. Older or newer brokers may not support
 * certain client features.  For instance, the transactional APIs need broker versions 0.11.0 or later. You will receive an
 * <code>UnsupportedVersionException</code> when invoking an API that is not available in the running broker version.
 * 此客户端可以与版本为0.10.0或更新的代理进行通信。较老或较新的代理可能不支持某些客户端特性。
 * 例如，事务性api需要代理版本0.11.0或更高版本。在调用在运行代理版本中不可用的API时，将收到UnsupportedVersionException。
 * </p>
 */
public class KafkaProducer<K, V> implements Producer<K, V> {

    private final Logger log;
    private static final AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
    private static final String JMX_PREFIX = "kafka.producer";
    public static final String NETWORK_THREAD_PREFIX = "kafka-producer-network-thread";

    private final String clientId;
    // Visible for testing
    final Metrics metrics;
    private final Partitioner partitioner;
    private final int maxRequestSize;
    private final long totalMemorySize;
    private final Metadata metadata;
    private final RecordAccumulator accumulator;
    private final Sender sender;
    private final Thread ioThread;
    private final CompressionType compressionType;
    private final Sensor errors;
    private final Time time;
    private final ExtendedSerializer<K> keySerializer;
    private final ExtendedSerializer<V> valueSerializer;
    private final ProducerConfig producerConfig;
    private final long maxBlockTimeMs;
    private final int requestTimeoutMs;
    private final ProducerInterceptors<K, V> interceptors;
    private final ApiVersions apiVersions;
    private final TransactionManager transactionManager;
    private TransactionalRequestResult initTransactionsResult;

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>. Values can be
     * either strings or Objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>
     *     通过提供一组键-值对作为配置来实例化生产者。这里记录了有效的配置字符串。
     *     值可以是字符串或适当类型的对象(例如，数字配置可以接受字符串42或整数42)。
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * 注意:在创建一个{@code KafkaProducer}你必须总是{@link #关闭()},以避免资源泄漏。
     * @param configs   The producer configs
     *
     */
    public KafkaProducer(final Map<String, Object> configs) {
        this(new ProducerConfig(configs), null, null, null, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * Values can be either strings or Objects of the appropriate type (for example a numeric configuration would accept
     * either the string "42" or the integer 42).
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param configs   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     *                       在直接传入序列化器时，在生成器中不会调用configure()方法。
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(ProducerConfig.addSerializerToConfig(configs, keySerializer, valueSerializer)),
            keySerializer,
            valueSerializer,
            null,
            null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param properties   The producer configs
     */
    public KafkaProducer(Properties properties) {
        this(new ProducerConfig(properties), null, null, null, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param properties   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(ProducerConfig.addSerializerToConfig(properties, keySerializer, valueSerializer)),
                keySerializer, valueSerializer, null, null);
    }

    @SuppressWarnings("unchecked")
    // visible for testing 可见测试
    KafkaProducer(ProducerConfig config,
                  Serializer<K> keySerializer,
                  Serializer<V> valueSerializer,
                  Metadata metadata,
                  KafkaClient kafkaClient) {
        try {
            Map<String, Object> userProvidedConfigs = config.originals();
            this.producerConfig = config;
            this.time = Time.SYSTEM;
            String clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
            if (clientId.length() <= 0)
                clientId = "producer-" + PRODUCER_CLIENT_ID_SEQUENCE.getAndIncrement();
            this.clientId = clientId;

            String transactionalId = userProvidedConfigs.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG) ?
                    (String) userProvidedConfigs.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG) : null;
            LogContext logContext;
            if (transactionalId == null)
                logContext = new LogContext(String.format("[Producer clientId=%s] ", clientId));
            else
                logContext = new LogContext(String.format("[Producer clientId=%s, transactionalId=%s] ", clientId, transactionalId));
            log = logContext.logger(KafkaProducer.class);
            log.trace("Starting the Kafka producer");

            Map<String, String> metricTags = Collections.singletonMap("client-id", clientId);
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                    .recordLevel(Sensor.RecordingLevel.forName(config.getString(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                    .tags(metricTags);
            List<MetricsReporter> reporters = config.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                    MetricsReporter.class);
            reporters.add(new JmxReporter(JMX_PREFIX));
            this.metrics = new Metrics(metricConfig, reporters, time);
            ProducerMetrics metricsRegistry = new ProducerMetrics(this.metrics);
            this.partitioner = config.getConfiguredInstance(ProducerConfig.PARTITIONER_CLASS_CONFIG, Partitioner.class);
            long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            if (keySerializer == null) {
                this.keySerializer = ensureExtended(config.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                                                                         Serializer.class));
                this.keySerializer.configure(config.originals(), true);
            } else {
                config.ignore(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
                this.keySerializer = ensureExtended(keySerializer);
            }
            if (valueSerializer == null) {
                this.valueSerializer = ensureExtended(config.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                                                                           Serializer.class));
                this.valueSerializer.configure(config.originals(), false);
            } else {
                config.ignore(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
                this.valueSerializer = ensureExtended(valueSerializer);
            }

            // load interceptors and make sure they get clientId
            userProvidedConfigs.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            List<ProducerInterceptor<K, V>> interceptorList = (List) (new ProducerConfig(userProvidedConfigs, false)).getConfiguredInstances(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                    ProducerInterceptor.class);
            this.interceptors = new ProducerInterceptors<>(interceptorList);
            ClusterResourceListeners clusterResourceListeners = configureClusterResourceListeners(keySerializer, valueSerializer, interceptorList, reporters);
            this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
            this.totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);
            this.compressionType = CompressionType.forName(config.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));

            this.maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG);
            this.requestTimeoutMs = config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            this.transactionManager = configureTransactionState(config, logContext, log);
            int retries = configureRetries(config, transactionManager != null, log);
            int maxInflightRequests = configureInflightRequests(config, transactionManager != null);
            short acks = configureAcks(config, transactionManager != null, log);

            this.apiVersions = new ApiVersions();
            this.accumulator = new RecordAccumulator(logContext,
                    config.getInt(ProducerConfig.BATCH_SIZE_CONFIG),
                    this.totalMemorySize,
                    this.compressionType,
                    config.getLong(ProducerConfig.LINGER_MS_CONFIG),
                    retryBackoffMs,
                    metrics,
                    time,
                    apiVersions,
                    transactionManager);
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
            if (metadata != null) {
                this.metadata = metadata;
            } else {
                this.metadata = new Metadata(retryBackoffMs, config.getLong(ProducerConfig.METADATA_MAX_AGE_CONFIG),
                    true, true, clusterResourceListeners);
                this.metadata.update(Cluster.bootstrap(addresses), Collections.<String>emptySet(), time.milliseconds());
            }
            ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config);
            Sensor throttleTimeSensor = Sender.throttleTimeSensor(metricsRegistry.senderMetrics);
            KafkaClient client = kafkaClient != null ? kafkaClient : new NetworkClient(
                    new Selector(config.getLong(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                            this.metrics, time, "producer", channelBuilder, logContext),
                    this.metadata,
                    clientId,
                    maxInflightRequests,
                    config.getLong(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                    config.getLong(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                    config.getInt(ProducerConfig.SEND_BUFFER_CONFIG),
                    config.getInt(ProducerConfig.RECEIVE_BUFFER_CONFIG),
                    this.requestTimeoutMs,
                    time,
                    true,
                    apiVersions,
                    throttleTimeSensor,
                    logContext);
            this.sender = new Sender(logContext,
                    client,
                    this.metadata,
                    this.accumulator,
                    maxInflightRequests == 1,
                    config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG),
                    acks,
                    retries,
                    metricsRegistry.senderMetrics,
                    Time.SYSTEM,
                    this.requestTimeoutMs,
                    config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG),
                    this.transactionManager,
                    apiVersions);
            String ioThreadName = NETWORK_THREAD_PREFIX + " | " + clientId;
            this.ioThread = new KafkaThread(ioThreadName, this.sender, true);
            this.ioThread.start();
            this.errors = this.metrics.sensor("errors");
            config.logUnused();
            AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics);
            log.debug("Kafka producer started");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed this is to prevent resource leak. see KAFKA-2121
            close(0, TimeUnit.MILLISECONDS, true);
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka producer", t);
        }
    }

    private static TransactionManager configureTransactionState(ProducerConfig config, LogContext logContext, Logger log) {

        TransactionManager transactionManager = null;

        boolean userConfiguredIdempotence = false;
        if (config.originals().containsKey(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG))
            userConfiguredIdempotence = true;

        boolean userConfiguredTransactions = false;
        if (config.originals().containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG))
            userConfiguredTransactions = true;

        boolean idempotenceEnabled = config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);

        if (!idempotenceEnabled && userConfiguredIdempotence && userConfiguredTransactions)
            throw new ConfigException("Cannot set a " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " without also enabling idempotence.");

        if (userConfiguredTransactions)
            idempotenceEnabled = true;

        if (idempotenceEnabled) {
            String transactionalId = config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
            int transactionTimeoutMs = config.getInt(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
            long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            transactionManager = new TransactionManager(logContext, transactionalId, transactionTimeoutMs, retryBackoffMs);
            if (transactionManager.isTransactional())
                log.info("Instantiated a transactional producer.");
            else
                log.info("Instantiated an idempotent producer.");
        }

        return transactionManager;
    }

    private static int configureRetries(ProducerConfig config, boolean idempotenceEnabled, Logger log) {
        boolean userConfiguredRetries = false;
        if (config.originals().containsKey(ProducerConfig.RETRIES_CONFIG)) {
            userConfiguredRetries = true;
        }
        if (idempotenceEnabled && !userConfiguredRetries) {
            // We recommend setting infinite retries when the idempotent producer is enabled, so it makes sense to make
            // this the default.
            log.info("Overriding the default retries config to the recommended value of {} since the idempotent " +
                    "producer is enabled.", Integer.MAX_VALUE);
            return Integer.MAX_VALUE;
        }
        if (idempotenceEnabled && config.getInt(ProducerConfig.RETRIES_CONFIG) == 0) {
            throw new ConfigException("Must set " + ProducerConfig.RETRIES_CONFIG + " to non-zero when using the idempotent producer.");
        }
        return config.getInt(ProducerConfig.RETRIES_CONFIG);
    }

    private static int configureInflightRequests(ProducerConfig config, boolean idempotenceEnabled) {
        if (idempotenceEnabled && 5 < config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)) {
            throw new ConfigException("Must set " + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + " to at most 5" +
                    " to use the idempotent producer.");
        }
        return config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
    }

    private static short configureAcks(ProducerConfig config, boolean idempotenceEnabled, Logger log) {
        boolean userConfiguredAcks = false;
        short acks = (short) parseAcks(config.getString(ProducerConfig.ACKS_CONFIG));
        if (config.originals().containsKey(ProducerConfig.ACKS_CONFIG)) {
            userConfiguredAcks = true;
        }

        if (idempotenceEnabled && !userConfiguredAcks) {
            log.info("Overriding the default {} to all since idempotence is enabled.", ProducerConfig.ACKS_CONFIG);
            return -1;
        }

        if (idempotenceEnabled && acks != -1) {
            throw new ConfigException("Must set " + ProducerConfig.ACKS_CONFIG + " to all in order to use the idempotent " +
                    "producer. Otherwise we cannot guarantee idempotence.");
        }
        return acks;
    }

    private static int parseAcks(String acksString) {
        try {
            return acksString.trim().equalsIgnoreCase("all") ? -1 : Integer.parseInt(acksString.trim());
        } catch (NumberFormatException e) {
            throw new ConfigException("Invalid configuration value for 'acks': " + acksString);
        }
    }

    /**
     * Needs to be called before any other methods when the transactional.id is set in the configuration.
     * 事务处理时需要在任何其他方法之前调用。id在配置中设置。
     *
     * This method does the following:  此方法执行以下操作
     *   1. Ensures any transactions initiated by previous instances of the producer with the same
     *      transactional.id are completed. If the previous instance had failed with a transaction in
     *      progress, it will be aborted. If the last transaction had begun completion,
     *      but not yet finished, this method awaits its completion.
     *      确保由生产者的前一个实例以相同的事务启动的任何事务。id完成。
     *      如果前面的实例在事务进行中失败，则将中止该实例。如果最后一个事务已开始完成，但尚未完成，则此方法等待完成。
     *   2. Gets the internal producer id and epoch, used in all future transactional
     *      messages issued by the producer.
     *      获取内部生成器id和epoch，该id和epoch用于由生成器发出的所有未来事务消息。
     *
     * Note that this method will raise {@link TimeoutException} if the transactional state cannot
     * be initialized before expiration of {@code max.block.ms}.
     * 注意，如果在{@code max.block.ms}到期前无法初始化事务状态，此方法将引发{@link TimeoutException}。
     * Additionally, it will raise {@link InterruptException}
     * if interrupted. It is safe to retry in either case, but once the transactional state has been successfully
     * initialized, this method should no longer be used.
     * 此外，如果被中断，它将引发{@link InterruptException}。
     * 在任何一种情况下重试都是安全的，但是一旦成功初始化了事务状态，就不应该再使用这个方法。
     *
     * @throws IllegalStateException if no transactional.id has been configured 如果没有事务异常，则为IllegalStateException。已配置id
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     *         指示代理不支持事务的致命错误(例如，如果其版本低于0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     *         指示已配置事务性的致命错误。id未被授权。有关更多细节，请参见异常
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     * @throws TimeoutException if the time taken for initialize the transaction has surpassed <code>max.block.ms</code>.
     * @throws InterruptException if the thread is interrupted while blocked    如果线程在阻塞时中断
     */
    public void initTransactions() {
        throwIfNoTransactionManager();
        if (initTransactionsResult == null) {
            initTransactionsResult = transactionManager.initializeTransactions();
            sender.wakeup();
        }

        try {
            if (initTransactionsResult.await(maxBlockTimeMs, TimeUnit.MILLISECONDS)) {
                initTransactionsResult = null;
            } else {
                throw new TimeoutException("Timeout expired while initializing transactional state in " + maxBlockTimeMs + "ms.");
            }
        } catch (InterruptedException e) {
            throw new InterruptException("Initialize transactions interrupted.", e);
        }
    }

    /**
     * Should be called before the start of each new transaction. Note that prior to the first invocation
     * of this method, you must invoke {@link #initTransactions()} exactly one time.
     * 应该在每个新事务开始之前调用。注意，在第一次调用该方法之前，必须一次调用{@link #initTransactions()}。
     *
     * @throws IllegalStateException if no transactional.id has been configured or if {@link #initTransactions()}
     *         has not yet been invoked
     * @throws ProducerFencedException if another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     */
    public void beginTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        transactionManager.beginTransaction();
    }

    /**
     * Sends a list of specified offsets to the consumer group coordinator, and also marks
     * those offsets as part of the current transaction. These offsets will be considered
     * committed only if the transaction is committed successfully.
     * 向使用者组协调器发送指定的偏移量列表，并将这些偏移量标记为当前事务的一部分。只有当事务成功提交时，才会考虑提交这些偏移量。
     * The committed offset should be the next message your application will consume, i.e. lastProcessedMessageOffset + 1.
     * 提交的偏移量应该是应用程序将使用的下一个消息，即lastProcessedMessageOffset + 1。
     * <p>
     * This method should be used when you need to batch consumed and produced messages
     * together, typically in a consume-transform-produce pattern. Thus, the specified
     * {@code consumerGroupId} should be the same as config parameter {@code group.id} of the used
     * {@link KafkaConsumer consumer}.
     * 当您需要将使用的和生成的消息批处理在一起时，应该使用此方法，通常是在使用-转换-生成模式时。
     * 因此，指定的{@code consumerGroupId}应该与配置参数{@code group相同。使用的{@link KafkaConsumer使用者}的id}。
     * Note, that the consumer should have {@code enable.auto.commit=false}
     * and should also not commit offsets manually (via {@link KafkaConsumer#commitSync(Map) sync} or
     * {@link KafkaConsumer#commitAsync(Map, OffsetCommitCallback) async} commits).
     * 注意，使用者应该有{@code enable.auto.commit=false}，并且不应该手动提交偏移量(通过{@link KafkaConsumer#commitSync(Map) sync}
     * 或{@link KafkaConsumer#commitAsync(Map, OffsetCommitCallback)异步提交)。
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.UnsupportedForMessageFormatException  fatal error indicating the message
     *         format used for the offsets topic on the broker does not support transactions
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
     *         other unexpected error
     */
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         String consumerGroupId) throws ProducerFencedException {
        throwIfNoTransactionManager();
        TransactionalRequestResult result = transactionManager.sendOffsetsToTransaction(offsets, consumerGroupId);
        sender.wakeup();
        result.await();
    }

    /**
     * Commits the ongoing transaction. This method will flush any unsent records before actually committing the transaction.
     *提交正在进行的事务。此方法将在实际提交事务之前刷新所有未发送的记录。
     * Further, if any of the {@link #send(ProducerRecord)} calls which were part of the transaction hit irrecoverable
     * errors, this method will throw the last received exception immediately and the transaction will not be committed.
     * 此外，如果作为事务一部分的{@link #send(ProducerRecord)}调用中出现了不可恢复的错误，该方法将立即抛出最后一个接收到的异常，并且不会提交事务。
     * So all {@link #send(ProducerRecord)} calls in a transaction must succeed in order for this method to succeed.
     * 因此，事务中的所有{@link #send(ProducerRecord)}调用必须成功，才能使该方法成功。
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
     *         other unexpected error
     */
    public void commitTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        TransactionalRequestResult result = transactionManager.beginCommit();
        sender.wakeup();
        result.await();
    }

    /**
     * Aborts the ongoing transaction. Any unflushed produce messages will be aborted when this call is made.
     * 中止正在进行的事务。在执行此调用时，任何未刷新的生成消息都将中止。
     * This call will throw an exception immediately if any prior {@link #send(ProducerRecord)} calls failed with a
     * {@link ProducerFencedException} or an instance of {@link org.apache.kafka.common.errors.AuthorizationException}.
     * 如果之前的{@link #send(ProducerRecord)}调用失败，{@link ProducerFencedException}
     * 或{@link org.apache.kafka.common.errors.AuthorizationException}，此调用将立即抛出异常。
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     */
    public void abortTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        TransactionalRequestResult result = transactionManager.beginAbort();
        sender.wakeup();
        result.await();
    }

    /**
     * Asynchronously send a record to a topic. Equivalent to <code>send(record, null)</code>.
     * 异步地将记录发送到主题。相当于发送(记录，空)。
     * See {@link #send(ProducerRecord, Callback)} for details.
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    /**
     * Asynchronously send a record to a topic and invoke the provided callback when the send has been acknowledged.
     * 异步地将记录发送到主题，并在确认发送后调用提供的回调。
     * <p>
     * The send is asynchronous and this method will return immediately once the record has been stored in the buffer of
     * records waiting to be sent. This allows sending many records in parallel without blocking to wait for the
     * response after each one.
     * 发送是异步的，一旦记录存储在等待发送的记录的缓冲区中，此方法将立即返回。这允许并行发送许多记录，而不阻塞在每个记录之后等待响应。
     * <p>
     * The result of the send is a {@link RecordMetadata} specifying the partition the record was sent to, the offset
     * it was assigned and the timestamp of the record. If
     * {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime} is used by the topic, the timestamp
     * will be the user provided timestamp or the record send time if the user did not specify a timestamp for the
     * record.
     * 发送的结果是{@link RecordMetadata}指定记录发送到的分区、分配的偏移量和记录的时间戳。
     * 如果主题使用{@link org.apache.kafka.common.record.TimestampType#CREATE TIME CreateTime}，
     * 时间戳将是用户提供的时间戳或记录发送时间(如果用户没有为记录指定时间戳)。
     * If {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime} is used for the
     * topic, the timestamp will be the Kafka broker local time when the message is appended.
     * 如果主题使用{@link org.apache.kafka.common.record.TimestampType#LOG APPEND TIME LogAppendTime}，
     * 那么当消息被附加时，时间戳将是Kafka代理本地时间。
     * <p>
     * Since the send call is asynchronous it returns a {@link java.util.concurrent.Future Future} for the
     * {@link RecordMetadata} that will be assigned to this record. Invoking {@link java.util.concurrent.Future#get()
     * get()} on this future will block until the associated request completes and then return the metadata for the record
     * or throw any exception that occurred while sending the record.
     * 由于send调用是异步的，所以它返回一个{@link java.util.concurrent。将分配给此记录的{@link RecordMetadata}的Future Future}。
     * 在这个future上调用{@link java.util.concurrent.Future#get() get()}将阻塞，直到关联的请求完成，
     * 然后返回记录的元数据，或者在发送记录时抛出任何异常。
     * <p>
     * If you want to simulate a simple blocking call you can call the <code>get()</code> method immediately:
     * 如果您想模拟一个简单的阻塞调用，您可以立即调用get()方法
     *
     * <pre>
     * {@code
     * byte[] key = "key".getBytes();
     * byte[] value = "value".getBytes();
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("my-topic", key, value)
     * producer.send(record).get();
     * }</pre>
     * <p>
     * Fully non-blocking usage can make use of the {@link Callback} parameter to provide a callback that
     * will be invoked when the request is complete.
     * 完全非阻塞的用法可以使用{@link Callback}参数来提供一个回调，该回调将在请求完成时被调用。
     *
     * <pre>
     * {@code
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("the-topic", key, value);
     * producer.send(myRecord,
     *               new Callback() {
     *                   public void onCompletion(RecordMetadata metadata, Exception e) {
     *                       if(e != null) {
     *                          e.printStackTrace();
     *                       } else {
     *                          System.out.println("The offset of the record we just sent is: " + metadata.offset());
     *                       }
     *                   }
     *               });
     * }
     * </pre>
     *
     * Callbacks for records being sent to the same partition are guaranteed to execute in order. That is, in the
     * following example <code>callback1</code> is guaranteed to execute before <code>callback2</code>:
     * 被发送到同一分区的记录的回调保证按顺序执行。也就是说，在下面的示例中，callback1保证在callback2之前执行
     *
     * <pre>
     * {@code
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key1, value1), callback1);
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key2, value2), callback2);
     * }
     * </pre>
     * <p>
     * When used as part of a transaction, it is not necessary to define a callback or check the result of the future
     * in order to detect errors from <code>send</code>. If any of the send calls failed with an irrecoverable error,
     * the final {@link #commitTransaction()} call will fail and throw the exception from the last failed send. When
     * this happens, your application should call {@link #abortTransaction()} to reset the state and continue to send
     * data.
     * 当作为事务的一部分使用时，不需要定义回调或检查将来的结果，以便从codesendcode中检测错误。
     * 如果任何发送调用失败并出现不可恢复的错误，则最终的{@link #commitTransaction()}调用将失败，并从上一次失败的发送抛出异常。
     * 发生这种情况时，应用程序应该调用{@link #abortTransaction()}来重置状态并继续发送数据
     * </p>
     * <p>
     * Some transactional send errors cannot be resolved with a call to {@link #abortTransaction()}.  In particular,
     * if a transactional send finishes with a {@link ProducerFencedException}, a {@link org.apache.kafka.common.errors.OutOfOrderSequenceException},
     * a {@link org.apache.kafka.common.errors.UnsupportedVersionException}, or an
     * {@link org.apache.kafka.common.errors.AuthorizationException}, then the only option left is to call {@link #close()}.
     * Fatal errors cause the producer to enter a defunct state in which future API calls will continue to raise
     * the same underyling error wrapped in a new {@link KafkaException}.
     * 一些事务发送错误不能通过调用{@link #abortTransaction()}来解决。特别是，
     * 如果事务发送用{@link producerfenceception}、{@link org.apache.kafcana.common.errors.outofordersequenceexception}、
     * {@link org.apache.common.unsupportedversionexception}或{@link org.apache.kafka.common.errors.AuthorizationException}结束，
     * 那么剩下的唯一选项就是调用{@link #close()}。
     * 致命错误会导致生产者进入一个失效状态，在这个状态中，将来的API调用将继续在一个新的{@li中引发同样的底层错误
     * </p>
     * <p>
     * It is a similar picture when idempotence is enabled, but no <code>transactional.id</code> has been configured.
     * In this case, {@link org.apache.kafka.common.errors.UnsupportedVersionException} and
     * {@link org.apache.kafka.common.errors.AuthorizationException} are considered fatal errors. However,
     * {@link ProducerFencedException} does not need to be handled. Additionally, it is possible to continue
     * sending after receiving an {@link org.apache.kafka.common.errors.OutOfOrderSequenceException}, but doing so
     * can result in out of order delivery of pending messages. To ensure proper ordering, you should close the
     * producer and create a new instance.
     * 当启用了幂等性，但不支持codetransactional时，情况与此类似。已配置idcode。
     * 在这种情况下，{@link org.apache.kafka.common.errors.UnsupportedVersionException}和{@link org.apache.kafka.common.errors.AuthorizationException}
     * 被认为是致命错误。然而，{@link ProducerFencedException}不需要处理。另外，在接收到{@link org.apache.kafka.common.errors.OutOfOrderSequenceException}后，还可以继续发送，
     * 但这样做可能会导致挂起消息的订单交付失效。为了确保正确的排序，你应该克洛斯
     * </p>
     * <p>
     * If the message format of the destination topic is not upgraded to 0.11.0.0, idempotent and transactional
     * produce requests will fail with an {@link org.apache.kafka.common.errors.UnsupportedForMessageFormatException}
     * error. If this is encountered during a transaction, it is possible to abort and continue. But note that future
     * sends to the same topic will continue receiving the same exception until the topic is upgraded.
     * 如果目标主题的消息格式没有升级到0.11.0.0，那么幂等和事务性生成请求将失败，并导致{@link org.apache.kafka.common.error . unsupportedformessageformatexception}错误。如果在事务期间遇到这种情况，则可以中止并继续。
     * 但是请注意，将来发送到相同主题的邮件将继续接收相同的异常，直到主题升级为止。
     * </p>
     * <p>
     * Note that callbacks will generally execute in the I/O thread of the producer and so should be reasonably fast or
     * they will delay the sending of messages from other threads. If you want to execute blocking or computationally
     * expensive callbacks it is recommended to use your own {@link java.util.concurrent.Executor} in the callback body
     * to parallelize processing.
     * 注意，回调通常在生产者的IO线程中执行，因此应该相当快，否则会延迟从其他线程发送消息。
     * 如果你想执行阻塞或计算昂贵的回调，建议使用你自己的{@link java.util.concurrent。回调体中的Executor}以并行化处理。
     *
     * @param record The record to send 要发送的记录
     * @param callback A user-supplied callback to execute when the record has been acknowledged by the server (null
     *        indicates no callback)
     *
     * @throws AuthenticationException if authentication fails. See the exception for more details
     * @throws AuthorizationException fatal error indicating that the producer is not allowed to write
     * @throws IllegalStateException if a transactional.id has been configured and no transaction has been started, or
     *                               when send is invoked after producer has been closed.
     * @throws InterruptException If the thread is interrupted while blocked
     * @throws SerializationException If the key or value are not valid objects given the configured serializers
     * @throws TimeoutException If the time taken for fetching metadata or allocating memory for the record has surpassed <code>max.block.ms</code>.
     * @throws KafkaException If a Kafka related error occurs that does not belong to the public API exceptions.
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        // intercept the record, which can be potentially modified; this method does not throw exceptions
        ProducerRecord<K, V> interceptedRecord = this.interceptors.onSend(record);
        return doSend(interceptedRecord, callback);
    }

    // Verify that this producer instance has not been closed. This method throws IllegalStateException if the producer
    // has already been closed.
    private void throwIfProducerClosed() {
        if (ioThread == null || !ioThread.isAlive())
            throw new IllegalStateException("Cannot perform operation after producer has been closed");
    }

    /**
     * Implementation of asynchronously send a record to a topic.
     * 异步发送记录到主题的实现。
     */
    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        TopicPartition tp = null;
        try {
            throwIfProducerClosed();
            // first make sure the metadata for the topic is available
            // 首先确保主题的元数据可用
            ClusterAndWaitTime clusterAndWaitTime;  //集群和等待时间
            try {
                clusterAndWaitTime = waitOnMetadata(record.topic(), record.partition(), maxBlockTimeMs);
            } catch (KafkaException e) {
                if (metadata.isClosed())
                    throw new KafkaException("Producer closed while send in progress", e);
                throw e;
            }
            long remainingWaitMs = Math.max(0, maxBlockTimeMs - clusterAndWaitTime.waitedOnMetadataMs);
            Cluster cluster = clusterAndWaitTime.cluster;
            // 序列化key
            byte[] serializedKey;
            try {
                serializedKey = keySerializer.serialize(record.topic(), record.headers(), record.key());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert key of class " + record.key().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in key.serializer", cce);
            }
            // 序列化value
            byte[] serializedValue;
            try {
                serializedValue = valueSerializer.serialize(record.topic(), record.headers(), record.value());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert value of class " + record.value().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in value.serializer", cce);
            }
            //求分区
            int partition = partition(record, serializedKey, serializedValue, cluster);
            tp = new TopicPartition(record.topic(), partition);

            setReadOnly(record.headers());
            Header[] headers = record.headers().toArray();

            int serializedSize = AbstractRecords.estimateSizeInBytesUpperBound(apiVersions.maxUsableProduceMagic(),
                    compressionType, serializedKey, serializedValue, headers);
            ensureValidRecordSize(serializedSize);
            long timestamp = record.timestamp() == null ? time.milliseconds() : record.timestamp();
            log.trace("Sending record {} with callback {} to topic {} partition {}", record, callback, record.topic(), partition);
            // producer callback will make sure to call both 'callback' and interceptor callback
            Callback interceptCallback = new InterceptorCallback<>(callback, this.interceptors, tp);

            if (transactionManager != null && transactionManager.isTransactional())
                transactionManager.maybeAddPartitionToTransaction(tp);

            RecordAccumulator.RecordAppendResult result = accumulator.append(tp, timestamp, serializedKey,
                    serializedValue, headers, interceptCallback, remainingWaitMs);
            if (result.batchIsFull || result.newBatchCreated) {
                log.trace("Waking up the sender since topic {} partition {} is either full or getting a new batch", record.topic(), partition);
                this.sender.wakeup();
            }
            return result.future;
            // handling exceptions and record the errors;   处理异常并记录错误
            // for API exceptions return them in the future,    对于API异常，在将来返回它们
            // for other exceptions throw directly
        } catch (ApiException e) {
            log.debug("Exception occurred during message send:", e);
            if (callback != null)
                callback.onCompletion(null, e);
            this.errors.record();
            this.interceptors.onSendError(record, tp, e);
            return new FutureFailure(e);
        } catch (InterruptedException e) {
            this.errors.record();
            this.interceptors.onSendError(record, tp, e);
            throw new InterruptException(e);
        } catch (BufferExhaustedException e) {
            this.errors.record();
            this.metrics.sensor("buffer-exhausted-records").record();
            this.interceptors.onSendError(record, tp, e);
            throw e;
        } catch (KafkaException e) {
            this.errors.record();
            this.interceptors.onSendError(record, tp, e);
            throw e;
        } catch (Exception e) {
            // we notify interceptor about all exceptions, since onSend is called before anything else in this method
            this.interceptors.onSendError(record, tp, e);
            throw e;
        }
    }

    private void setReadOnly(Headers headers) {
        if (headers instanceof RecordHeaders) {
            ((RecordHeaders) headers).setReadOnly();
        }
    }

    /**
     * Wait for cluster metadata including partitions for the given topic to be available.
     * 等待集群元数据(包括给定主题的分区)可用。
     * @param topic The topic we want metadata for  我们需要元数据的主题
     * @param partition A specific partition expected to exist in metadata, or null if there's no preference
     *                  期望在元数据中存在特定的分区，如果没有首选项，则为null
     * @param maxWaitMs The maximum time in ms for waiting on the metadata  在ms中等待元数据的最长时间
     * @return The cluster containing topic metadata and the amount of time we waited in ms
     * 集群包含主题元数据和我们在ms中等待的时间
     * @throws KafkaException for all Kafka-related exceptions, including the case where this method is called after producer close
     */
    private ClusterAndWaitTime waitOnMetadata(String topic, Integer partition, long maxWaitMs) throws InterruptedException {
        // add topic to metadata topic list if it is not there already and reset expiry
        // 将主题添加到元数据主题列表(如果还没有的话)，并重置到期时间
        metadata.add(topic);
        Cluster cluster = metadata.fetch();
        Integer partitionsCount = cluster.partitionCountForTopic(topic);
        // Return cached metadata if we have it, and if the record's partition is either undefined
        // or within the known partition range
        // 如果我们有缓存的元数据，如果记录的分区没有定义或者在已知的分区范围内，返回它
        if (partitionsCount != null && (partition == null || partition < partitionsCount))
            return new ClusterAndWaitTime(cluster, 0);

        long begin = time.milliseconds();
        long remainingWaitMs = maxWaitMs;
        long elapsed;
        // Issue metadata requests until we have metadata for the topic or maxWaitTimeMs is exceeded.
        // 发出元数据请求，直到超出主题的元数据或maxWaitTimeMs。
        // In case we already have cached metadata for the topic, but the requested partition is greater
        // than expected, issue an update request only once. This is necessary in case the metadata
        // is stale and the number of partitions for this topic has increased in the meantime.
        // 如果我们已经为主题缓存了元数据，但是请求的分区大于预期，那么只发出一次更新请求
        // 这是必要的，以防元数据过时，同时此主题的分区数量增加。
        do {
            log.trace("Requesting metadata update for topic {}.", topic);
            metadata.add(topic);
            int version = metadata.requestUpdate();
            sender.wakeup();
            try {
                metadata.awaitUpdate(version, remainingWaitMs);
            } catch (TimeoutException ex) {
                // Rethrow with original maxWaitMs to prevent logging exception with remainingWaitMs
                // 重新抛出原来的maxWaitMs，以防止日志记录异常
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            }
            cluster = metadata.fetch();
            elapsed = time.milliseconds() - begin;
            if (elapsed >= maxWaitMs)
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            if (cluster.unauthorizedTopics().contains(topic))
                throw new TopicAuthorizationException(topic);
            remainingWaitMs = maxWaitMs - elapsed;
            partitionsCount = cluster.partitionCountForTopic(topic);
        } while (partitionsCount == null);

        if (partition != null && partition >= partitionsCount) {
            throw new KafkaException(
                    String.format("Invalid partition given with record: %d is not in the range [0...%d).", partition, partitionsCount));
        }

        return new ClusterAndWaitTime(cluster, elapsed);
    }

    /**
     * Validate that the record size isn't too large    验证记录大小是否太大
     */
    private void ensureValidRecordSize(int size) {
        if (size > this.maxRequestSize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than the maximum request size you have configured with the " +
                    ProducerConfig.MAX_REQUEST_SIZE_CONFIG +
                    " configuration.");
        if (size > this.totalMemorySize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than the total memory buffer you have configured with the " +
                    ProducerConfig.BUFFER_MEMORY_CONFIG +
                    " configuration.");
    }

    /**
     * Invoking this method makes all buffered records immediately available to send (even if <code>linger.ms</code> is
     * greater than 0) and blocks on the completion of the requests associated with these records. The post-condition
     * of <code>flush()</code> is that any previously sent record will have completed (e.g. <code>Future.isDone() == true</code>).
     * A request is considered completed when it is successfully acknowledged
     * according to the <code>acks</code> configuration you have specified or else it results in an error.
     * 调用此方法可使所有缓冲记录立即可用以发送(即使是延迟发送)。ms大于0)，并阻塞与这些记录相关联的请求的完成。
     * flush()的后置条件是以前发送的任何记录都已经完成。
     * 如果根据您指定的acks配置成功确认请求，则认为请求已经完成，否则会导致错误。
     * <p>
     * Other threads can continue sending records while one thread is blocked waiting for a flush call to complete,
     * 当一个线程被阻塞时，其他线程可以继续发送记录，等待刷新调用完成
     * however no guarantee is made about the completion of records sent after the flush call begins.
     * 然而，不能保证在启动调用开始后发送的记录是否完成。
     * <p>
     * This method can be useful when consuming from some input system and producing into Kafka. The <code>flush()</code>
     * call gives a convenient way to ensure all previously sent messages have actually completed.
     * 这种方法在从输入系统中进行消费并生成卡夫卡时非常有用。
     * 调用提供了一种方便的方式来确保所有以前发送的消息都已实际完成
     * <p>
     * This example shows how to consume from one Kafka topic and produce to another Kafka topic:
     * 这个例子展示了如何从一个卡夫卡主题消费到另一个卡夫卡主题
     * <pre>
     * {@code
     * for(ConsumerRecord<String, String> record: consumer.poll(100))
     *     producer.send(new ProducerRecord("my-topic", record.key(), record.value());
     * producer.flush();
     * consumer.commit();
     * }
     * </pre>
     *
     * Note that the above example may drop records if the produce request fails. If we want to ensure that this does not occur
     * we need to set <code>retries=&lt;large_number&gt;</code> in our config.
     * 注意，如果生成请求失败，上面的示例可能会删除记录。如果我们想确保不发生这种情况，我们需要设置重试=<large number>在我们的配置。
     * </p>
     * <p>
     * Applications don't need to call this method for transactional producers, since the {@link #commitTransaction()} will
     * flush all buffered records before performing the commit. This ensures that all the {@link #send(ProducerRecord)}
     * calls made since the previous {@link #beginTransaction()} are completed before the commit.
     * 应用程序不需要为事务生产者调用此方法，因为{@link #commitTransaction()}将在执行提交之前刷新所有缓冲记录。
     * 这确保了所有{@link #send(ProducerRecord)}调用都是在提交之前完成的。
     * </p>
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public void flush() {
        log.trace("Flushing accumulated records in producer.");
        this.accumulator.beginFlush();
        this.sender.wakeup();
        try {
            this.accumulator.awaitFlushCompletion();
        } catch (InterruptedException e) {
            throw new InterruptException("Flush interrupted.", e);
        }
    }

    /**
     * Get the partition metadata for the given topic. This can be used for custom partitioning.
     * 获取给定主题的分区元数据。这可以用于自定义分区。
     * @throws AuthenticationException if authentication fails. See the exception for more details
     * @throws AuthorizationException if not authorized to the specified topic. See the exception for more details
     * @throws InterruptException if the thread is interrupted while blocked
     * @throws TimeoutException if metadata could not be refreshed within {@code max.block.ms}
     * @throws KafkaException for all Kafka-related exceptions, including the case where this method is called after producer close
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        Objects.requireNonNull(topic, "topic cannot be null");
        try {
            return waitOnMetadata(topic, null, maxBlockTimeMs).cluster.partitionsForTopic(topic);
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    /**
     * Get the full set of internal metrics maintained by the producer.
     * 获得生产者维护的完整的内部度量。
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    /**
     * Close this producer. This method blocks until all previously sent requests complete.
     * 关闭这个生产商。此方法将阻塞，直到所有以前发送的请求完成为止。
     * This method is equivalent to <code>close(Long.MAX_VALUE, TimeUnit.MILLISECONDS)</code>.
     * 这种方法相当于长密码子。最大价值,TimeUnit.MILLISECONDS)代码。
     * <p>
     * <strong>If close() is called from {@link Callback}, a warning message will be logged and close(0, TimeUnit.MILLISECONDS)
     * will be called instead. We do this because the sender thread would otherwise try to join itself and
     * block forever.</strong>
     * 如果close()从{@link回调}调用，将记录一条警告消息，并将调用close(0, TimeUnit.MILLISECONDS)。
     * 我们这样做是因为发送方线程会试图连接自己并永远阻塞。
     * <p>
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public void close() {
        close(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    /**
     * This method waits up to <code>timeout</code> for the producer to complete the sending of all incomplete requests.
     * 此方法等待超时，以便生产者完成所有不完整请求的发送。
     * <p>
     * If the producer is unable to complete all requests before the timeout expires, this method will fail
     * any unsent and unacknowledged records immediately.
     * 如果生产者无法在超时过期之前完成所有请求，此方法将立即导致任何未发送和未确认的记录失败。
     * <p>
     * If invoked from within a {@link Callback} this method will not block and will be equivalent to
     * <code>close(0, TimeUnit.MILLISECONDS)</code>. This is done since no further sending will happen while
     * blocking the I/O thread of the producer.
     * 如果从{@link回调}中调用，此方法将不会阻塞，并且将等效于close(0, TimeUnit.MILLISECONDS)。
     * 这样做是因为在阻塞生产者的IO线程时不会发生进一步的发送。
     *
     * @param timeout The maximum time to wait for producer to complete any pending requests. The value should be
     *                non-negative. Specifying a timeout of zero means do not wait for pending send requests to complete.
     *                等待生产者完成任何未决请求的最长时间。这个值应该是非负的。指定超时为零意味着不要等待等待的发送请求完成。
     * @param timeUnit The time unit for the <code>timeout</code>
     * @throws InterruptException If the thread is interrupted while blocked
     * @throws IllegalArgumentException If the <code>timeout</code> is negative.
     */
    @Override
    public void close(long timeout, TimeUnit timeUnit) {
        close(timeout, timeUnit, false);
    }

    private void close(long timeout, TimeUnit timeUnit, boolean swallowException) {
        if (timeout < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");

        log.info("Closing the Kafka producer with timeoutMillis = {} ms.", timeUnit.toMillis(timeout));
        // this will keep track of the first encountered exception
        // 这将跟踪第一个遇到的异常
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        boolean invokedFromCallback = Thread.currentThread() == this.ioThread;
        if (timeout > 0) {
            if (invokedFromCallback) {
                log.warn("Overriding close timeout {} ms to 0 ms in order to prevent useless blocking due to self-join. " +
                        "This means you have incorrectly invoked close with a non-zero timeout from the producer call-back.", timeout);
            } else {
                // Try to close gracefully.
                if (this.sender != null)
                    this.sender.initiateClose();
                if (this.ioThread != null) {
                    try {
                        this.ioThread.join(timeUnit.toMillis(timeout));
                    } catch (InterruptedException t) {
                        firstException.compareAndSet(null, new InterruptException(t));
                        log.error("Interrupted while joining ioThread", t);
                    }
                }
            }
        }

        if (this.sender != null && this.ioThread != null && this.ioThread.isAlive()) {
            log.info("Proceeding to force close the producer since pending requests could not be completed " +
                    "within timeout {} ms.", timeout);
            this.sender.forceClose();
            // Only join the sender thread when not calling from callback.
            if (!invokedFromCallback) {
                try {
                    this.ioThread.join();
                } catch (InterruptedException e) {
                    firstException.compareAndSet(null, new InterruptException(e));
                }
            }
        }

        ClientUtils.closeQuietly(interceptors, "producer interceptors", firstException);
        ClientUtils.closeQuietly(metrics, "producer metrics", firstException);
        ClientUtils.closeQuietly(keySerializer, "producer keySerializer", firstException);
        ClientUtils.closeQuietly(valueSerializer, "producer valueSerializer", firstException);
        ClientUtils.closeQuietly(partitioner, "producer partitioner", firstException);
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);
        log.debug("Kafka producer has been closed");
        Throwable exception = firstException.get();
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close kafka producer", exception);
        }
    }

    private ClusterResourceListeners configureClusterResourceListeners(Serializer<K> keySerializer, Serializer<V> valueSerializer, List<?>... candidateLists) {
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
        for (List<?> candidateList: candidateLists)
            clusterResourceListeners.maybeAddAll(candidateList);

        clusterResourceListeners.maybeAdd(keySerializer);
        clusterResourceListeners.maybeAdd(valueSerializer);
        return clusterResourceListeners;
    }

    /**
     * computes partition for given record. 计算给定记录的分区。
     * if the record has partition returns the value otherwise
     * calls configured partitioner class to compute the partition.
     * 如果记录有分区，则返回值，否则调用配置的partitioner类来计算分区
     */
    private int partition(ProducerRecord<K, V> record, byte[] serializedKey, byte[] serializedValue, Cluster cluster) {
        Integer partition = record.partition();
        return partition != null ?
                partition :
                partitioner.partition(
                        record.topic(), record.key(), serializedKey, record.value(), serializedValue, cluster);
    }

    private void throwIfNoTransactionManager() {
        if (transactionManager == null)
            throw new IllegalStateException("Cannot use transactional methods without enabling transactions " +
                    "by setting the " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " configuration property");
    }

    private static class ClusterAndWaitTime {
        final Cluster cluster;
        final long waitedOnMetadataMs;
        ClusterAndWaitTime(Cluster cluster, long waitedOnMetadataMs) {
            this.cluster = cluster;
            this.waitedOnMetadataMs = waitedOnMetadataMs;
        }
    }

    private static class FutureFailure implements Future<RecordMetadata> {

        private final ExecutionException exception;

        public FutureFailure(Exception exception) {
            this.exception = new ExecutionException(exception);
        }

        @Override
        public boolean cancel(boolean interrupt) {
            return false;
        }

        @Override
        public RecordMetadata get() throws ExecutionException {
            throw this.exception;
        }

        @Override
        public RecordMetadata get(long timeout, TimeUnit unit) throws ExecutionException {
            throw this.exception;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

    }

    /**
     * A callback called when producer request is complete. It in turn calls user-supplied callback (if given) and
     * notifies producer interceptors about the request completion.
     * 当生产者请求完成时调用的回调。它反过来调用用户提供的回调(如果给定的话)并通知生产者拦截器请求完成。
     */
    private static class InterceptorCallback<K, V> implements Callback {
        private final Callback userCallback;
        private final ProducerInterceptors<K, V> interceptors;
        private final TopicPartition tp;

        private InterceptorCallback(Callback userCallback, ProducerInterceptors<K, V> interceptors, TopicPartition tp) {
            this.userCallback = userCallback;
            this.interceptors = interceptors;
            this.tp = tp;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            metadata = metadata != null ? metadata : new RecordMetadata(tp, -1, -1, RecordBatch.NO_TIMESTAMP, Long.valueOf(-1L), -1, -1);
            this.interceptors.onAcknowledgement(metadata, exception);
            if (this.userCallback != null)
                this.userCallback.onCompletion(metadata, exception);
        }
    }
}
