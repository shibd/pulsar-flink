package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

/**
 * Tests for {@link FlinkPulsarTransactionalSink}
 */
public class FlinkPulsarTransactionalSinkTest extends PulsarTestBaseWithFlink{
    @Test
    public void testCreateTransactionalSink() throws Exception {
        String tp = newTopic();
        ClientConfigurationData clientConfigurationData = new ClientConfigurationData();
        clientConfigurationData.setServiceUrl(serviceUrl);
        FlinkPulsarTransactionalSink<String> sink = new FlinkPulsarTransactionalSink<String>(
                adminUrl,
                new HashMap<>(),
                new Properties(),
                clientConfigurationData,
                Optional.of(tp),
                TopicKeyExtractor.NULL,
                String.class,
                RecordSchemaType.AVRO,
                FlinkPulsarTransactionalSink.Semantic.EXACTLY_ONCE,
                1
        );
        Assert.assertNotNull(sink);
        sink.beginTransaction();

    }

    @Test
    public void createPulsarAdmin() throws Exception{
        PulsarClient build = PulsarClient.builder().serviceUrl(serviceUrl).build();
        PulsarAdmin.builder().serviceHttpUrl(serviceUrl).build();
    }
}
