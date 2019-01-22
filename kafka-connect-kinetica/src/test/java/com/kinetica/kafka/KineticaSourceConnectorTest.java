package com.kinetica.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/*
 * Tests SourceConnector for badly and well-defined configurations
 */
public class KineticaSourceConnectorTest {
    @ClassRule
    public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private KineticaSourceConnector connector;
    private Map<String, String> connProps;

    @Before
    public void setup() throws Exception {
        connector = new KineticaSourceConnector();

        Map<String, String> cfg = TestConnector.configureConnection(
                TestConnector.getConfig("config/quickstart-kinetica-sink.properties"));
        cfg.put(KineticaSourceConnectorConfig.PARAM_TABLE_NAMES, "table1,table2");
        cfg.put(KineticaSourceConnectorConfig.PARAM_TOPIC_PREFIX, "topic_");

        connProps = new HashMap<>(cfg);
    }

    @Test(expected = ConnectException.class)
    public void nullProperties() {
        connector.start(null);
    }
    
    @Test(expected = ConnectException.class)
    public void expectedKineticaUrl() {
        Map<String, String> testProps = new HashMap<>(connProps);
        testProps.remove(KineticaSourceConnectorConfig.PARAM_URL);
        connector.start(testProps);
    }

    @Test
    public void minimunConfig() {
        connector.start(connProps);
        connector.stop();
    }

    @Test
    public void checkTaskClass() {
        assertEquals(KineticaSourceTask.class, connector.taskClass());
    }

    @Test(expected = ConnectException.class)
    public void configTasksWithoutStart() {
        connector.taskConfigs(1);
    }

    @Test(expected = ConnectException.class) 
    public void invalidConfigTaskNumber() {
        connector.start(connProps);
        connector.taskConfigs(0);
    }

    @Test
    public void configTasks() {
        connector.start(connProps);
        int uris = connProps.get(KineticaSourceConnectorConfig.PARAM_TABLE_NAMES).split(",").length;
        for (int index=1; index<=connProps.get(KineticaSourceConnectorConfig.PARAM_TABLE_NAMES).split(",").length; index++) {
            List<Map<String, String>> taskConfigs = connector.taskConfigs(index);
            assertTrue(taskConfigs.size() == (index > uris ? uris : index));
        }
        connector.stop();
    }

    @Test
    public void checkVersion() {
        assertNotNull(connector.version());
        assertFalse("unknown".equalsIgnoreCase(connector.version()));
    }

    @Test
    public void checkDefaultConf() {
        assertNotNull(connector.config());
    }
}
