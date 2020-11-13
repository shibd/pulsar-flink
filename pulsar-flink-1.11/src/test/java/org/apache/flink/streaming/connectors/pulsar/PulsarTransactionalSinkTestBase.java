package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.pulsar.PulsarTestBaseWithFlink;
import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;
import org.apache.flink.streaming.connectors.pulsar.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.connectors.pulsar.testutils.IntegerSource;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.test.util.TestUtils;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.shaded.org.bouncycastle.util.Integers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

@Slf4j
public class PulsarTransactionalSinkTestBase {
    private PulsarAdmin admin;
    private final static String CLUSTER_NAME = "standalone";
    private final static String TENANT = "tnx";
    private final static String NAMESPACE1 = TENANT + "/ns1";
    private final static String TOPIC_OUTPUT = NAMESPACE1 + "/output";
    private final static String TOPIC_MESSAGE_ACK_TEST = NAMESPACE1 + "/message-ack-test";
    private final static String adminUrlStand = "http://localhost:8080";
    private final static String serviceUrlStand = "pulsar://localhost:6650";
    /**
     * Tests the exactly-once semantic for the simple writes into Kafka.
     */
    @Test
    public void testExactlyOnceRegularSink() throws Exception {
        admin = PulsarAdmin.builder().serviceHttpUrl(adminUrlStand).build();
        //admin.clusters().createCluster(CLUSTER_NAME, new ClusterData(adminUrl));
        //List<String> tenants = admin.tenants().getTenants();
        //admin.tenants().createTenant(TENANT,
        //        new TenantInfo(Sets.newHashSet("app1"), Sets.newHashSet(CLUSTER_NAME)));
        //admin.namespaces().createNamespace(NAMESPACE1);
        //admin.topics().createPartitionedTopic(TOPIC_OUTPUT, TOPIC_PARTITION);
        //admin.topics().createPartitionedTopic(TOPIC_MESSAGE_ACK_TEST, TOPIC_PARTITION);

        /*admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfo(Sets.newHashSet("app1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 16);*/

        testExactlyOnce(true, 1);
    }

    protected void testExactlyOnce(boolean regularSink, int sinksCount) throws Exception {
        final String topic = "oneToOneTopicSink-new-multip";
        final int partition = 0;
        final int numElements = 1000;
        final int failAfterElements = 333;

        TypeInformationSerializationSchema<Integer> schema = new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        // process exactly failAfterElements number of elements and then shutdown Pulsar broker and fail application
        List<Integer> expectedElements = getIntegersSequence(numElements);

        DataStream<Integer> inputStream = env
                .addSource(new IntegerSource(numElements))
                .map(new FailingIdentityMapper<Integer>(failAfterElements));

        for (int i = 0; i < sinksCount; i++) {
           /* FlinkKafkaPartitioner<Integer> partitioner = new FlinkKafkaPartitioner<Integer>() {
                @Overridex
                public int partition(Integer record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
                    return partition;
                }
            };*/

            ClientConfigurationData clientConfigurationData = new ClientConfigurationData();
            clientConfigurationData.setServiceUrl(serviceUrlStand);
            SinkFunction<Integer> sink = new FlinkPulsarTransactionalSink<Integer>(
                    adminUrlStand,
                    new HashMap<>(),
                    new Properties(),
                    clientConfigurationData,
                    Optional.of(topic),
                    null,
                    Integer.class,
                    RecordSchemaType.AVRO,
                    Schema.INT32,
                    FlinkPulsarTransactionalSink.Semantic.EXACTLY_ONCE,
                    1
            );
            inputStream.addSink(sink).setParallelism(3);
        }

        FailingIdentityMapper.failedBefore = false;
        TestUtils.tryExecute(env, "Exactly once test");
        for (int i = 0; i < sinksCount; i++) {
            // assert that before failure we successfully snapshot/flushed all expected elements
            /*assertExactlyOnceForTopic(
                    topic,
                    partition,
                    expectedElements,
                    60000L);*/
            //deleteTestTopic(topic + i);
        }

    }

    @Test
    public void testTransaction() throws Exception{
        PulsarClient client = PulsarClient.builder().enableTransaction(true).serviceUrl(serviceUrlStand).build();
        Thread.sleep(1000);
        //PulsarAdmin.builder().serviceHttpUrl(serviceUrlStand).build();
        Transaction transaction = ((PulsarClientImpl) client).newTransaction().withTransactionTimeout(1, TimeUnit.HOURS).build().get();
        ProducerImpl<Integer> sinkProducer = (ProducerImpl<Integer>) client.newProducer(Schema.INT32)
                .topic("test-sink-topic8")
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();

        for (int i = 0; i < 5; i++) {
            sinkProducer.newMessage(transaction).value(i).sendAsync().get();
        }
        Consumer<Integer> sinkConsumer = client.newConsumer(Schema.INT32)
                .topic("test-sink-topic8")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("test")
                .subscribe();
        Message<Integer> sinkMessage = sinkConsumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(sinkMessage);

        transaction.commit().get();

        sinkMessage = sinkConsumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNotNull(sinkMessage);
        System.out.println(sinkMessage.getValue());
        sinkMessage = sinkConsumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNotNull(sinkMessage);
        System.out.println(sinkMessage.getValue());
    }

    private List<Integer> getIntegersSequence(int size) {
        List<Integer> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            result.add(i);
        }
        return result;
    }

    /**
     * We manually handle the timeout instead of using JUnit's timeout to return failure instead of timeout error.
     * After timeout we assume that there are missing records and there is a bug, not that the test has run out of time.
     */
    public void assertExactlyOnceForTopic(
            String topic,
            int partition,
            List<Integer> expectedElements,
            long timeoutMillis) throws Exception{

        long startMillis = System.currentTimeMillis();
        List<Integer> actualElements = new ArrayList<>();

        // until we timeout...
            PulsarClient client = PulsarClient.builder().enableTransaction(true).serviceUrl(serviceUrlStand).build();
            Consumer<Integer> test = client
                    .newConsumer(Schema.INT32)
                    .topic(topic)
                    .subscriptionName("test-new1")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    //.subscriptionType(SubscriptionType.Shared)
                    //.enableBatchIndexAcknowledgment(true)
                    .subscribe();
        while (System.currentTimeMillis() < startMillis + timeoutMillis) {
            // query pulsar for new records ...
            Message<Integer> message = test.receive();
            log.info("consume the message {} with the value {}", message.getMessageId(), message.getValue());
            actualElements.add(message.getValue());
            // succeed if we got all expectedElements
            if (actualElements.equals(expectedElements)) {
                return;
            }
            // fail early if we already have too many elements
            if (actualElements.size() > expectedElements.size()) {
                break;
            }
        }

        fail(String.format("Expected %s, but was: %s", formatElements(expectedElements), formatElements(actualElements)));
    }

    @Test
    public void testSchema(){
        Schema<Integer> avro = Schema.AVRO(Integer.class);
        System.out.println(avro);

    }
    private String formatElements(List<Integer> elements) {
        if (elements.size() > 50) {
            return String.format("number of elements: <%s>", elements.size());
        }
        else {
            return String.format("elements: <%s>", elements);
        }
    }

    // ------------------------------------------------------------------------

    /**
     * Mapper that validates partitioning and maps to partition.
     */
    public static class PartitionValidatingMapper extends RichMapFunction<Tuple2<Long, String>, Integer> {

        private final int numPartitions;

        private int ourPartition = -1;

        public PartitionValidatingMapper(int numPartitions) {
            this.numPartitions = numPartitions;
        }

        @Override
        public Integer map(Tuple2<Long, String> value) throws Exception {
            int partition = value.f0.intValue() % numPartitions;
            if (ourPartition != -1) {
                assertEquals("inconsistent partitioning", ourPartition, partition);
            } else {
                ourPartition = partition;
            }
            return partition;
        }
    }

    /**
     * Sink that validates records received from each partition and checks that there are no duplicates.
     */
    public static class PartitionValidatingSink implements SinkFunction<Integer> {
        private final int[] valuesPerPartition;

        public PartitionValidatingSink(int numPartitions) {
            this.valuesPerPartition = new int[numPartitions];
        }

        @Override
        public void invoke(Integer value) throws Exception {
            valuesPerPartition[value]++;

            boolean missing = false;
            for (int i : valuesPerPartition) {
                if (i < 100) {
                    missing = true;
                    break;
                }
            }
            if (!missing) {
                throw new SuccessException();
            }
        }
    }

    private static class BrokerRestartingMapper<T> extends RichMapFunction<T, T>
            implements CheckpointedFunction, CheckpointListener {

        private static final long serialVersionUID = 6334389850158707313L;

        public static volatile boolean triggeredShutdown;
        public static volatile int lastSnapshotedElementBeforeShutdown;
        public static volatile Runnable shutdownAction;

        private final int failCount;
        private int numElementsTotal;

        private boolean failer;

        public static void resetState(Runnable shutdownAction) {
            triggeredShutdown = false;
            lastSnapshotedElementBeforeShutdown = 0;
            BrokerRestartingMapper.shutdownAction = shutdownAction;
        }

        public BrokerRestartingMapper(int failCount) {
            this.failCount = failCount;
        }

        @Override
        public void open(Configuration parameters) {
            failer = getRuntimeContext().getIndexOfThisSubtask() == 0;
        }

        @Override
        public T map(T value) throws Exception {
            numElementsTotal++;
            Thread.sleep(10);

            if (!triggeredShutdown && failer && numElementsTotal >= failCount) {
                // shut down a Kafka broker
                triggeredShutdown = true;
                shutdownAction.run();
            }
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (!triggeredShutdown) {
                lastSnapshotedElementBeforeShutdown = numElementsTotal;
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
        }
    }

    private static final class InfiniteIntegerSource implements SourceFunction<Integer> {

        private volatile boolean running = true;
        private int counter = 0;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                ctx.collect(counter++);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
