package com.kinetica.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.Map;

/*
 * Tests that Connector Config has well-defined properties
 */
public class KineticaSourceConnectorConfigTest {
    private  Map<String,String> sourceConfig = null;

    @Before
    public void setup() throws Exception {
        this.sourceConfig = TestConnector.getConfig("config/quickstart-kinetica-source.properties");
    }

    @Test
    public void checkDocumentation() {
        ConfigDef config = KineticaSourceConnectorConfig.config;
        for (String key : config.names()){
            assertFalse("Property " + key + " should be documented",
                    config.configKeys().get(key).documentation == null ||
                            "".equals(config.configKeys().get(key).documentation.trim()));
        }
    }

    @Test
    public void toRst() {
        assertNotNull(KineticaSourceConnectorConfig.config.toRst());
    }
    
    @Test(expected = ConfigException.class)
    public void requiredURLMissing() {
        Map<String, String> props = TestConnector.configureConnection(sourceConfig);
        props.remove(KineticaSourceConnectorConfig.PARAM_URL);
        KineticaSourceConnectorConfig config = new KineticaSourceConnectorConfig(KineticaSourceConnectorConfig.config, props);
        
    }

    @Test
    public void nameTest(){
        Map<String, String> props = TestConnector.configureConnection(sourceConfig);
        props.put(KineticaSourceConnectorConfig.PARAM_TABLE_NAMES, "table1, table2");
        props.put(KineticaSourceConnectorConfig.PARAM_SCHEMA_VERSION, "1,1");

        KineticaSourceConnectorConfig config = new KineticaSourceConnectorConfig(KineticaSourceConnectorConfig.config, props);
        assertNotNull(config.getConnectorName());
    }

    @Test(expected = ConnectException.class)
    public void validateVersionsTest1() {
        Map<String, String> props = TestConnector.configureConnection(sourceConfig);
        props.put(KineticaSourceConnectorConfig.PARAM_TABLE_NAMES, "table1, table2");
        props.put(KineticaSourceConnectorConfig.PARAM_SCHEMA_VERSION, "1,2,3");
        
        KineticaSourceConnectorConfig config = new KineticaSourceConnectorConfig(KineticaSourceConnectorConfig.config, props);
        
    }

    @Test
    public void validateVersionsTest2() {
        Map<String, String> props = TestConnector.configureConnection(sourceConfig);
        props.put(KineticaSourceConnectorConfig.PARAM_TABLE_NAMES, "table1, table2, table3");
        props.put(KineticaSourceConnectorConfig.PARAM_SCHEMA_VERSION, "1,2,3");
        
        KineticaSourceConnectorConfig config = new KineticaSourceConnectorConfig(KineticaSourceConnectorConfig.config, props);
        
    }
    

    @Test
    public void validateVersionsTest3() {
        Map<String, String> props = TestConnector.configureConnection(sourceConfig);
        props.put(KineticaSourceConnectorConfig.PARAM_TABLE_NAMES, "table1, table2");
        props.put(KineticaSourceConnectorConfig.PARAM_SCHEMA_VERSION, "");
        
        KineticaSourceConnectorConfig config = new KineticaSourceConnectorConfig(KineticaSourceConnectorConfig.config, props);
        
    }
}