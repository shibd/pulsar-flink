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

import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;

import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;

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
                1,
                3600000
        );
        Assert.assertNotNull(sink);
        try{
            sink.beginTransaction();
        }catch (Exception e){
            Assert.fail("Expecting to initialize transaction correctly, but something went wrong");
        }
    }
}
