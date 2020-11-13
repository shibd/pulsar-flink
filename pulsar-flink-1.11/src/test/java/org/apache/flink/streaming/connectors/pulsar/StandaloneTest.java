package org.apache.flink.streaming.connectors.pulsar;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.junit.Test;

public class StandaloneTest {
    private static final String serviceURL = "pulsar://localhost:6651";

    @Test
    public void testAdmin() throws Exception{
        PulsarAdmin.builder().serviceHttpUrl(serviceURL).build();
    }
}
