/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;
import org.apache.flink.streaming.connectors.pulsar.internal.CachedPulsarClient;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.util.ExceptionUtils;

import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.transaction.TransactionCoordinatorClientImpl;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Slf4j
public class FlinkPulsarTransactionalSink<IN> extends TwoPhaseCommitSinkFunction<IN, FlinkPulsarTransactionalSink.PulsarTransactionState<IN>, Void> {
    /**
     * Semantics that can be chosen.
     * <li>{@link #EXACTLY_ONCE}</li>
     * <li>{@link #AT_LEAST_ONCE}</li>
     * <li>{@link #NONE}</li>
     */
    public enum Semantic {

        /**
         * Semantic.EXACTLY_ONCE the Flink sink will write all messages in a Pulsar transaction that will be
         * committed to Pulsar on a checkpoint.
         * <p>
         * Between each checkpoint a Pulsar transaction is created, which is committed on
         * {@link FlinkPulsarTransactionalSink#notifyCheckpointComplete(long)}.
         * To decrease the chance of failing checkpoints there are four options:
         * <li>decrease number of max concurrent checkpoints</li>
         * <li>make checkpoints more reliable (so that they complete faster)</li>
         * <li>increase the delay between checkpoints</li>
         */
        EXACTLY_ONCE,

        /**
         * Semantic.AT_LEAST_ONCE the Flink sink will wait for all outstanding messages in the Pulsar buffers
         * to be acknowledged by the Pulsar producer on a checkpoint.
         */
        AT_LEAST_ONCE,

        /**
         * Semantic.NONE means that nothing will be guaranteed. Messages can be lost and/or duplicated in case
         * of failure.
         */
        NONE
    }

    protected String adminUrl;

    protected ClientConfigurationData clientConfigurationData;

    protected final Map<String, Object> producerConf;

    protected final Properties properties;

    private transient PulsarClient pulsarClient;

    protected transient BiConsumer<MessageId, Throwable> sendCallback;

    protected boolean failOnWrite = true;

    protected transient volatile Throwable failedWrite;

    protected final TopicKeyExtractor<IN> topicKeyExtractor;

    private final Class<IN> recordClazz;

    /**
     * Type for serialized messages, default use AVRO.
     */
    private final RecordSchemaType schemaType;

    /**
     * Errors encountered in the async producer are stored here.
     */
    @Nullable
    protected transient volatile Exception asyncException;

    protected boolean flushOnCheckpoint = true;

    protected long maxBlockTimeMs;

    protected final String defaultTopic;

    /**
     * Number of unacknowledged records.
     */
    protected AtomicLong pendingRecords = new AtomicLong();

    protected ConcurrentHashMap<TxnID, List<MessageId>> tid2MessagesMap;

    protected ConcurrentHashMap<TxnID, List<CompletableFuture<MessageId>>> tid2FuturesMap;

    protected final long transactionTimeout;

    protected final boolean forcedTopic;

    /**
     * Semantic chosen for this instance.
     */
    protected FlinkPulsarTransactionalSink.Semantic semantic;

    protected transient Producer<?> singleProducer;

    protected transient Map<String, Producer<?>> topic2Producer;

    public FlinkPulsarTransactionalSink(
            String adminUrl,
            Map<String, Object> producerConf,
            Properties properties,
            ClientConfigurationData clientConf,
            Optional<String> defaultTopicName,
            TopicKeyExtractor<IN> topicKeyExtractor,
            Class<IN> recordClazz,
            RecordSchemaType recordSchemaType
    ) {
        this(adminUrl, producerConf, properties, clientConf,
                defaultTopicName, topicKeyExtractor, recordClazz,
                recordSchemaType, Semantic.AT_LEAST_ONCE);
    }

    public FlinkPulsarTransactionalSink(
            String adminUrl,
            Map<String, Object> producerConf,
            Properties properties,
            ClientConfigurationData clientConf,
            Optional<String> defaultTopicName,
            TopicKeyExtractor<IN> topicKeyExtractor,
            Class<IN> recordClazz,
            RecordSchemaType recordSchemaType,
            Semantic semantic
    ) {
        this(adminUrl, producerConf, properties, clientConf,
                defaultTopicName, topicKeyExtractor, recordClazz,
                recordSchemaType, semantic, PulsarOptions.TRANSACTION_TIMEOUT_MS.defaultValue(),
                PulsarOptions.TRANSACTION_MAX_BLOCK_TIME_MS.defaultValue());
    }

    public FlinkPulsarTransactionalSink(
            String adminUrl,
            Map<String, Object> producerConf,
            Properties properties,
            ClientConfigurationData clientConf,
            Optional<String> defaultTopicName,
            TopicKeyExtractor<IN> topicKeyExtractor,
            Class<IN> recordClazz,
            RecordSchemaType recordSchemaType,
            Semantic semantic,
            long transactionTimeout,
            long maxBlockTimeMs
    ) {
        super(new TransactionStateSerializer(), VoidSerializer.INSTANCE);
        this.adminUrl = adminUrl;
        this.transactionTimeout = transactionTimeout;
        this.producerConf = producerConf;
        this.properties = properties;
        this.clientConfigurationData = clientConf;
        this.recordClazz = recordClazz;
        this.schemaType = recordSchemaType;
        this.maxBlockTimeMs = maxBlockTimeMs;
        this.semantic = checkNotNull(semantic, "semantic is null");
        this.tid2MessagesMap = new ConcurrentHashMap<>();
        this.tid2FuturesMap = new ConcurrentHashMap<>();
        if (defaultTopicName.isPresent()) {
            this.forcedTopic = true;
            this.defaultTopic = defaultTopicName.get();
            this.topicKeyExtractor = topicKeyExtractor == null ?
                    TopicKeyExtractor.getRebalancedExtractor(defaultTopic) : topicKeyExtractor;
        } else {
            this.forcedTopic = false;
            this.defaultTopic = null;
            ClosureCleaner.clean(
                    topicKeyExtractor, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
            this.topicKeyExtractor = checkNotNull(topicKeyExtractor);
        }
        if (semantic == Semantic.EXACTLY_ONCE) {
            clientConfigurationData.setEnableTransaction(true);
        }
        this.pulsarClient = getOrCreatePulsarClient(clientConfigurationData);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (forcedTopic) {
            //uploadSchema(defaultTopic);
            singleProducer = createProducer(producerConf, defaultTopic, buildSchema(recordClazz, schemaType));
        } else {
            topic2Producer = new HashMap<>();
        }
        super.open(parameters);
    }

    private void acknowledgeMessage() {
        if (flushOnCheckpoint) {
            pendingRecords.decrementAndGet();
        }
    }

    protected void initializeSendCallback() {
        if (sendCallback != null) {
            return;
        }

        if (failOnWrite) {
            this.sendCallback = (messageId, throwable) -> {
                if (failedWrite == null && throwable == null) {
                    acknowledgeMessage();
                } else if (failedWrite == null && throwable != null) {
                    failedWrite = throwable;
                } else { // failedWrite != null
                    // do nothing and wait next checkForError to throw exception
                }
            };
        } else {
            this.sendCallback = (messageId, throwable) -> {
                if (failedWrite == null && throwable != null) {
                    log.error("Error while sending message to Pulsar: {}", ExceptionUtils.stringifyException(throwable));
                }
                acknowledgeMessage();
            };
        }
    }

    private Schema<IN> buildSchema(Class<IN> recordClazz, RecordSchemaType recordSchemaType) {
        if (recordSchemaType == null) {
            return Schema.AVRO(recordClazz);
        }
        switch (recordSchemaType) {
            case AVRO:
                return Schema.AVRO(recordClazz);
            case JSON:
                return Schema.JSON(recordClazz);
            default:
                throw new IllegalArgumentException("not support schema type " + recordSchemaType);
        }
    }

    protected PulsarClient getOrCreatePulsarClient(
            ClientConfigurationData clientConf) {
        try {
            return CachedPulsarClient
                    .getOrCreate(clientConf);
        } catch (ExecutionException e) {
            log.error("Failed to getOrCreate a PulsarClient");
            throw new RuntimeException(e);
        }

    }

    protected <R> Producer<R> getProducer(String topic) {
        log.debug("get producer for topic {}", topic);
        if (forcedTopic) {
            return (Producer<R>) singleProducer;
        }

        if (topic2Producer.containsKey(topic)) {
            return (Producer<R>) topic2Producer.get(topic);
        } else {
            //uploadSchema(topic);
            Producer p = createProducer(producerConf, topic, buildSchema(recordClazz, schemaType));
            topic2Producer.put(topic, p);
            return (Producer<R>) p;
        }
    }

    protected Producer<IN> createProducer(
            Map<String, Object> producerConf,
            String topic,
            Schema<IN> schema) {
        try {
            log.info("create producer for topic {}", topic);
            return getOrCreatePulsarClient(clientConfigurationData)
                    .newProducer(schema)
                    .topic(topic)
                    .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                    .batchingMaxMessages(5 * 1024 * 1024)
                    .sendTimeout(0, TimeUnit.SECONDS)
                    .loadConf(producerConf)
                    .create();
        } catch (PulsarClientException e) {
            log.error("Failed to create producer for topic {}", topic);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        super.snapshotState(context);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        log.info("initializeState...");
        if (semantic != Semantic.NONE && !((StreamingRuntimeContext) this.getRuntimeContext()).isCheckpointingEnabled()) {
            log.warn("Using {} semantic, but checkpointing is not enabled. Switching to {} semantic.", semantic, Semantic.NONE);
            semantic = Semantic.NONE;
        }

        super.initializeState(context);
    }

    @Override
    protected void recoverAndCommit(PulsarTransactionState<IN> transaction) {
        if (transaction.isTransactional()) {
            log.info("transaction {} is recoverAndCommit...", transaction.transactionalId);
            TransactionCoordinatorClientImpl tcClient = ((PulsarClientImpl) getOrCreatePulsarClient(clientConfigurationData)).getTcClient();
            TxnID transactionalId = transaction.transactionalId;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                tcClient.commit(transactionalId, transaction.pendingMessages);
            } catch (TransactionCoordinatorClientException.InvalidTxnStatusException statusException) {
                log.info("transaction {} is already commited...", transaction.transactionalId);
            } catch (TransactionCoordinatorClientException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected void recoverAndAbort(PulsarTransactionState<IN> transaction) {
        if (transaction.isTransactional()) {
            log.info("transaction {} is recoverAndAbort...", transaction.transactionalId);
            TransactionCoordinatorClientImpl tcClient = ((PulsarClientImpl) getOrCreatePulsarClient(clientConfigurationData)).getTcClient();
            TxnID transactionalId = transaction.transactionalId;
            try {
                tcClient.abort(transactionalId, transaction.pendingMessages);
                //TODO we should extract the condition of tnx state.
            } catch (TransactionCoordinatorClientException.InvalidTxnStatusException statusException) {
                log.info("transaction {} is already aborted...", transaction.transactionalId);
            } catch (TransactionCoordinatorClientException e) {
                throw new RuntimeException(e);
            }
        }
    }


    @Override
    protected void invoke(PulsarTransactionState<IN> transactionState, IN value, Context context) throws Exception {
        checkErroneous();
        initializeSendCallback();

        TypedMessageBuilder<IN> mb;

        if (forcedTopic) {
            if (transactionState.isTransactional()) {
                mb = ((ProducerImpl<IN>) getProducer(defaultTopic))
                        .newMessage(transactionState.transaction)
                        .value(value);
            } else {
                mb = ((ProducerImpl<IN>) getProducer(defaultTopic))
                        .newMessage()
                        .value(value);
            }
        } else {
            byte[] key = topicKeyExtractor.serializeKey(value);
            String topic = topicKeyExtractor.getTopic(value);

            if (topic == null) {
                if (failOnWrite) {
                    throw new NullPointerException("no topic present in the data.");
                }
                return;
            }

            if (transactionState.isTransactional()) {
                mb = ((ProducerImpl<IN>) getProducer(topic))
                        .newMessage(transactionState.transaction)
                        .value(value);
            } else {
                mb = ((ProducerImpl<IN>) getProducer(topic))
                        .newMessage()
                        .value(value);
            }
            if (key != null) {
                mb.keyBytes(key);
            }
        }

        if (flushOnCheckpoint) {
            pendingRecords.incrementAndGet();
        }
        CompletableFuture<MessageId> messageIdFuture = mb.sendAsync();
        if (transactionState.isTransactional()) {
            TxnID transactionalId = transactionState.transactionalId;
            List<CompletableFuture<MessageId>> futureList;
            if (tid2FuturesMap.get(transactionalId) == null) {
                futureList = new ArrayList<>();
                tid2FuturesMap.put(transactionalId, futureList);
            } else {
                futureList = tid2FuturesMap.get(transactionalId);
            }
            futureList.add(messageIdFuture);
            log.info("message {} is invoke in txn {}", value, transactionState.transactionalId);
        }
        messageIdFuture.whenComplete(sendCallback);
    }

    @Override
    protected PulsarTransactionState<IN> beginTransaction() throws Exception {
        switch (semantic) {
            case EXACTLY_ONCE:
                log.info("transaction is begining in EXACTLY_ONCE mode");
                Transaction transaction = createTransaction();
                long txnIdLeastBits = ((TransactionImpl) transaction).getTxnIdLeastBits();
                long txnIdMostBits = ((TransactionImpl) transaction).getTxnIdMostBits();
                TxnID txnID = new TxnID(txnIdMostBits, txnIdLeastBits);
                if (tid2MessagesMap.get(txnID) == null) {
                    tid2MessagesMap.put(txnID, new ArrayList<>());
                }
                if (tid2FuturesMap.get(txnID) == null) {
                    tid2FuturesMap.put(txnID, new ArrayList<>());
                }
                return new PulsarTransactionState<IN>(
                        new TxnID(txnIdMostBits, txnIdLeastBits),
                        transaction,
                        tid2MessagesMap.get(txnID));
            case AT_LEAST_ONCE:
            case NONE:
                // Do not create new producer on each beginTransaction() if it is not necessary
                final PulsarTransactionState<IN> currentTransaction = currentTransaction();
                if (currentTransaction != null && currentTransaction.transactionalId != null) {
                    return new PulsarTransactionState<IN>(
                            currentTransaction.transactionalId,
                            currentTransaction.getTransaction(),
                            currentTransaction.getPendingMessages());
                }
                return new PulsarTransactionState<IN>(null, null, new ArrayList<>());
            default:
                throw new UnsupportedOperationException("Not implemented semantic");
        }
    }

    @Override
    protected void preCommit(PulsarTransactionState<IN> transaction) throws Exception {
        switch (semantic) {
            case EXACTLY_ONCE:
            case AT_LEAST_ONCE:
                flush(transaction);
                break;
            case NONE:
                break;
            default:
                throw new UnsupportedOperationException("Not implemented semantic");
        }
        if (transaction.isTransactional()) {
            log.info("{} preCommit with pending message size {}", transaction.transactionalId, tid2MessagesMap.get(currentTransaction().transactionalId).size());
        } else {
            log.info("in AT_LEAST_ONCE mode, producer was flushed by preCommit");
        }
        checkErroneous();
    }

    @Override
    protected void commit(PulsarTransactionState<IN> transactionState) {
        if (transactionState.isTransactional()) {
            log.info("transaction {} is commiting", transactionState.transactionalId.toString());
            CompletableFuture<Void> future = transactionState.transaction.commit();
            try {
                future.get(maxBlockTimeMs, TimeUnit.MILLISECONDS);
                log.info("transaction {} is commited with messageID size {}", transactionState.transactionalId.toString(), tid2MessagesMap.get(transactionState.transactionalId).size());
                // TODO test clear the map is needed
                tid2MessagesMap.remove(transactionState.transactionalId);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected void abort(PulsarTransactionState<IN> transactionState) {
        if (transactionState.isTransactional()) {
            CompletableFuture<Void> future = transactionState.transaction.abort();
            log.info("transaction {} is aborting", transactionState.transactionalId.toString());
            try {
                future.get(maxBlockTimeMs, TimeUnit.MILLISECONDS);
                log.info("transaction {} is aborted", transactionState.transactionalId.toString());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected void checkErroneous() throws FlinkPulsarException {
        Exception e = asyncException;
        if (e != null) {
            // prevent double throwing
            asyncException = null;
            throw new FlinkPulsarException(
                    FlinkPulsarErrorCode.EXTERNAL_ERROR,
                    "Failed to send data to Kafka: " + e.getMessage(),
                    e);
        }
    }

    /**
     * For each checkpoint we create new {@link org.apache.pulsar.client.api.transaction.Transaction} so that new transactions will not clash
     * with transactions created during previous checkpoints.
     */
    private Transaction createTransaction() throws Exception {
        PulsarClientImpl client = (PulsarClientImpl) getOrCreatePulsarClient(clientConfigurationData);
        Thread.sleep(100);
        Transaction transaction = client
                .newTransaction()
                .withTransactionTimeout(transactionTimeout, TimeUnit.MILLISECONDS)
                .build()
                .get();

        //FlinkKafkaInternalProducer<byte[], byte[]> producer = initTransactionalProducer(transactionalId, true);
        //producer.initTransactions();
        return transaction;
    }

    /**
     * Flush pending records.
     *
     * @param transaction
     */
    private void flush(PulsarTransactionState<IN> transaction) throws FlinkPulsarException {
        //log.info("transaction {} is flush", transaction.transactionalId.toString());
        try {
            if (singleProducer != null) {
                singleProducer.flush();
            } else {
                if (topic2Producer != null) {
                    for (Producer<?> p : topic2Producer.values()) {
                        p.flush();
                    }
                }
            }
        } catch (PulsarClientException e) {
            throw new RuntimeException("flush failed");
        }
        if (transaction.isTransactional()) {
            // we check the future was completed and add the messageId to list for persistence.
            List<CompletableFuture<MessageId>> futureList = tid2FuturesMap.get(transaction.transactionalId);
            for (CompletableFuture<MessageId> future : futureList) {
                try {
                    MessageId messageId = future.get();
                    TxnID transactionalId = transaction.transactionalId;
                    List<MessageId> messageIdList;
                    if (tid2MessagesMap.get(transactionalId) == null) {
                        messageIdList = new ArrayList<>();
                        tid2MessagesMap.put(transactionalId, messageIdList);
                    } else {
                        messageIdList = tid2MessagesMap.get(transactionalId);
                    }
                    messageIdList.add(messageId);
                    log.info("transaction {} add the message {} to messageIdLIst", transactionalId, messageId);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if (pendingRecords.get() != 0) {
            throw new IllegalStateException("Pending record count must be zero at this point: " + pendingRecords.get());
        }

        // if the flushed requests has errors, we should propagate it also and fail the checkpoint
        checkErroneous();
    }

    /**
     * State for handling transactions.
     */
    @VisibleForTesting
    @Internal
    public static class PulsarTransactionState<T> {

        private final transient Transaction transaction;

        //private final transient Producer<T> producer;

        private final List<MessageId> pendingMessages;

        @Nullable
        final TxnID transactionalId;

        @VisibleForTesting
        public PulsarTransactionState() {
            this(null, null, new ArrayList<>());
        }

        @VisibleForTesting
        public PulsarTransactionState(@Nullable TxnID transactionalId,
                                      @Nullable Transaction transaction,
                                      //@Nullable Producer<T> producer,
                                      List<MessageId> pendingMessages) {
            this.transactionalId = transactionalId;
            this.transaction = transaction;
            //this.producer = producer;
            this.pendingMessages = pendingMessages;
        }

        public Transaction getTransaction() {
            return transaction;
        }

        boolean isTransactional() {
            return transactionalId != null;
        }

        public List<MessageId> getPendingMessages() {
            return pendingMessages;
        }

        @Override
        public String toString() {
            if (isTransactional()) {
                return String.format(
                        "%s [transactionalId=%s] [pendingMessages=%s]",
                        this.getClass().getSimpleName(),
                        transactionalId.toString(),
                        pendingMessages.size());
            } else {
                return String.format(
                        "%s this state is not in transactional mode",
                        this.getClass().getSimpleName());
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            PulsarTransactionState<?> that = (PulsarTransactionState<?>) o;

            if (!pendingMessages.equals(that.pendingMessages)) {
                return false;
            }
            return transactionalId != null ? transactionalId.equals(that.transactionalId) : that.transactionalId == null;
        }

        @Override
        public int hashCode() {
            int result = pendingMessages.hashCode();
            result = 31 * result + (transactionalId != null ? transactionalId.hashCode() : 0);
            return result;
        }
    }

    /**
     * Context associated to this instance of the {@link FlinkPulsarTransactionalSink}. User for keeping track of the
     * transactionalIds.
     */
    @VisibleForTesting
    @Internal
    public static class PulsarTransactionContext {


        @VisibleForTesting
        public PulsarTransactionContext() {

        }


    }

    /**
     * {@link org.apache.flink.api.common.typeutils.TypeSerializer} for
     * {@link PulsarTransactionState}.
     */
    @VisibleForTesting
    @Internal
    public static class TransactionStateSerializer<T> extends TypeSerializerSingleton<PulsarTransactionState<T>> {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public PulsarTransactionState<T> createInstance() {
            return null;
        }

        @Override
        public PulsarTransactionState<T> copy(PulsarTransactionState<T> from) {
            return from;
        }

        @Override
        public PulsarTransactionState<T> copy(
                PulsarTransactionState<T> from,
                PulsarTransactionState<T> reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(
                PulsarTransactionState<T> record,
                DataOutputView target) throws IOException {
            if (record.transactionalId == null) {
                target.writeBoolean(false);
            } else {
                target.writeBoolean(true);
                target.writeLong(record.transactionalId.getMostSigBits());
                target.writeLong(record.transactionalId.getLeastSigBits());
                int size = record.pendingMessages.size();
                target.writeInt(size);
                for (MessageId messageId : record.pendingMessages) {
                    byte[] messageData = messageId.toByteArray();
                    target.writeInt(messageData.length);
                    target.write(messageData);
                }
            }
        }

        @Override
        public PulsarTransactionState<T> deserialize(DataInputView source) throws IOException {
            TxnID transactionalId = null;
            List<MessageId> pendingMessages = new ArrayList<>();
            if (source.readBoolean()) {
                long mostSigBits = source.readLong();
                long leastSigBits = source.readLong();
                transactionalId = new TxnID(mostSigBits, leastSigBits);
                int size = source.readInt();
                for (int i = 0; i < size; i++) {
                    int length = source.readInt();
                    byte[] messageData = new byte[length];
                    source.read(messageData);
                    pendingMessages.add(MessageId.fromByteArray(messageData));
                }
            }
            return new PulsarTransactionState<T>(transactionalId, null, pendingMessages);
        }

        @Override
        public PulsarTransactionState<T> deserialize(
                PulsarTransactionState<T> reuse,
                DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(
                DataInputView source, DataOutputView target) throws IOException {
            boolean hasTransactionalId = source.readBoolean();
            target.writeBoolean(hasTransactionalId);
            if (hasTransactionalId) {
                long mostSigBits = source.readLong();
                long leastSigBits = source.readLong();
                target.writeLong(mostSigBits);
                target.writeLong(leastSigBits);
            }
        }

        // -----------------------------------------------------------------------------------

        @Override
        public TypeSerializerSnapshot<PulsarTransactionState<T>> snapshotConfiguration() {
            return new TransactionStateSerializerSnapshot<T>();
        }

        /**
         * Serializer configuration snapshot for compatibility and format evolution.
         */
        @SuppressWarnings("WeakerAccess")
        public static final class TransactionStateSerializerSnapshot<T> extends SimpleTypeSerializerSnapshot<PulsarTransactionState<T>> {

            public TransactionStateSerializerSnapshot() {
                super(TransactionStateSerializer::new);
            }
        }
    }
}
