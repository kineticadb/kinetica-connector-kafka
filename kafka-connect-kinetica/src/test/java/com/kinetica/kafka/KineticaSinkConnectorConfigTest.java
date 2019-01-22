package com.kinetica.kafka;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.Before;
import org.junit.Test;

/*
 * Testing if the ConnectorConfig has well-defined properties
 */
public class KineticaSinkConnectorConfigTest {

    private  Map<String,String> sinkConfig = null;

    @Before
    public void setup() throws Exception {
        this.sinkConfig = TestConnector.getConfig("config/quickstart-kinetica-sink.properties");
    }
    
    
    @Test
    public void testDocumentation() {
        ConfigDef config = KineticaSinkConnectorConfig.config;
        for (String key : config.names()){
            assertFalse("Property " + key + " should be documented",
                    config.configKeys().get(key).documentation == null ||
                            "".equals(config.configKeys().get(key).documentation.trim()));
        }
    }
    
    @Test
    public void toRst() {
        assertNotNull(KineticaSinkConnectorConfig.config.toRst());
    }

    @Test
    public void nameTest(){
        Map<String, String> props = TestConnector.configureConnection(sinkConfig);

        KineticaSinkConnectorConfig config = new KineticaSinkConnectorConfig(KineticaSinkConnectorConfig.config, props);
        assertNotNull(config.getConnectorName());
    }

    @Test(expected = ConnectException.class)
    public void validateOverrideTest1() {
        Map<String, String> props = TestConnector.configureConnection(sinkConfig);
        props.put(SinkTask.TOPICS_CONFIG, "topic");
        props.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, "tab1,tab2,tab3");
        
        KineticaSinkConnectorConfig config = new KineticaSinkConnectorConfig(KineticaSinkConnectorConfig.config, props);
        
    }

    @Test
    public void validateOverrideTest2() {
        Map<String, String> props = TestConnector.configureConnection(sinkConfig);
        props.put(SinkTask.TOPICS_CONFIG, "topic1,topic2, topic3");
        props.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, "tab1,tab2,tab3");
        
        KineticaSinkConnectorConfig config = new KineticaSinkConnectorConfig(KineticaSinkConnectorConfig.config, props);
        
    }

    @Test
    public void validateOverrideTest3() {
        Map<String, String> props = TestConnector.configureConnection(sinkConfig);
        props.put(SinkTask.TOPICS_CONFIG, "topic");
        props.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, "");
        
        KineticaSinkConnectorConfig config = new KineticaSinkConnectorConfig(KineticaSinkConnectorConfig.config, props);
        
    }
}
