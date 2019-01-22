package com.kinetica.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;

/*
 * Tests configurations for running a badly and well-defined SinkConnector
 */
public class KineticaSinkConnectorTest {
    private KineticaSinkConnector connector;
    private Map<String, String> connProps;
    
    @Before
    public void setup() throws Exception {
        connector = new KineticaSinkConnector();
        
        Map<String, String> cfg = TestConnector.configureConnection(
                TestConnector.getConfig("config/quickstart-kinetica-sink.properties"));
        cfg.put(KineticaSinkConnectorConfig.PARAM_COLLECTION, "");

        connProps = new HashMap<>(cfg);
    }
    
    // no config properties, expect fail
    @Test(expected = ConnectException.class)
    public void nullProperties() {
        connector.start(null);
    }
    
    // config properties have no Kinetica URL, expect fail
    @Test(expected = ConnectException.class)
    public void expectedKineticaUrl() {
        Map<String, String> testProps = new HashMap<>(connProps);
        testProps.remove(KineticaSinkConnectorConfig.PARAM_URL);
        connector.start(testProps);
    }
    
    // minimum config, expect pass
    @Test
    public void minimumConfig() {
        connector.start(connProps);
        connector.stop();
    }
    
    // check that Task class matches its Connector
    @Test
    public void checkTaskClass() {
        assertEquals(KineticaSinkTask.class, connector.taskClass());
    }
    
    // check that running tasks require starting the connector, expect fail
    @Test(expected = ConnectException.class)
    public void configTasksWithoutStart() {
        connector.taskConfigs(1);
    }

    // check that running connector can't have 0 tasks, expect fail 
    @Test(expected = ConnectException.class) 
    public void invalidConfigTaskNumber() {
        connector.start(connProps);
        connector.taskConfigs(0);
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