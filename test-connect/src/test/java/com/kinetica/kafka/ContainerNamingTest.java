package com.kinetica.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import com.gpudb.GPUdb;
import com.kinetica.kafka.data.utils.KafkaSchemaHelpers;
import com.kinetica.kafka.utils.ConnectorConfigHelper;

public class ContainerNamingTest {
    
private final static int TASK_NUM = 10;
private final static String DEFAULT_COLLECTION = "ki_home";
private final static String COLLECTION = "TEST__COLLECTION";
private final static String PREFIX = "default_schema_";
private final static String TABLE_OVERRIDE = "TEST_COLLECTION.test__table";

private HashMap<String, String> tableSizeProps;
private String[] tables = { "ki_home.default_schema_test_multidot_topic1",
        "TEST__COLLECTION.default_schema_test_multidot_topic2", 
        "TEST_COLLECTION.test__table",
        "ki_home.test_multidot_topic4"};

private GPUdb gpudb;
    @Before
    public void setup() throws Exception {
        this.gpudb = ConnectorConfigHelper.getGPUdb();
        this.tableSizeProps = new HashMap<String, String>();
        this.tableSizeProps.put("get_sizes", "true");
    }
    
    @After
    public void cleanup() throws Exception{
        ConnectorConfigHelper.tableCleanUp(this.gpudb, tables);
        this.gpudb = null;
    }
    
    @Test
    public void testTopicWithDefaultSchema() throws Exception {
        String topic = "test.multidot.topic1";
        // Connector is configured to have additional table prefix
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(topic, DEFAULT_COLLECTION, PREFIX, null, true, true);
        // Add uuid-based connector name
        config.put("name", "8fe3b81d-4b51-43a8-b41c-43e31ecf59c1");
        
        // Cleanup, configure and start connector and sinktask
        String tableName = ConnectorConfigHelper.getDestTable(config, this.gpudb, topic, null);
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector, topic); 

        runSinkTask(task, topic);
        
        String expectedTableName = DEFAULT_COLLECTION + "." + PREFIX +
                topic.replaceAll("[.]", KineticaSinkConnectorConfig.DEFAULT_DOT_REPLACEMENT);
        // Expect tableName to match PREFIX+topic name
        assertEquals(tableName, expectedTableName);
        
        // expect Kinetica table to exist
        boolean tableExists = this.gpudb.hasTable(tableName, null).getTableExists(); 
        assertTrue(tableExists);
        
        // expect table size to match number of Kafka messages generated/ingested
        int size = this.gpudb.showTable(tableName, tableSizeProps).getFullSizes().get(0).intValue();        
        assertEquals(size, TASK_NUM);
        
        // cleanup of test tables
        this.tables[0] = expectedTableName;

        connector.stop();
    }

    @Test
    public void testTopicWithDefinedSchema() throws Exception {
        String topic = "test.multidot.topic2";
        // Connector is configured to have additional table prefix
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(topic, COLLECTION, PREFIX, null, true, true);
        // Add uuid-based connector name
        config.put("name", "8fe3b81d-4b51-43a8-b41c-43e31ecf59c2");
        
        // Cleanup, configure and start connector and sinktask
        String tableName = ConnectorConfigHelper.getDestTable(config, this.gpudb, topic, null);
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector, topic); 

        runSinkTask(task, topic);
        
        String expectedTableName = COLLECTION + "." + PREFIX +
                topic.replaceAll("[.]", KineticaSinkConnectorConfig.DEFAULT_DOT_REPLACEMENT);
        // Expect tableName to match PREFIX+topic name
        assertEquals(tableName, expectedTableName);
        
        // expect Kinetica table to exist
        boolean tableExists = this.gpudb.hasTable(tableName, null).getTableExists(); 
        assertTrue(tableExists);
        
        // expect table size to match number of Kafka messages generated/ingested
        int size = this.gpudb.showTable(tableName, tableSizeProps).getFullSizes().get(0).intValue();        
        assertEquals(size, TASK_NUM);

        connector.stop();
    }

    @Test
    public void testTopicWithSchemaInOverride() throws Exception {
        String topic = "test.multidot.topic3";
        // Connector is configured to have additional table prefix
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(topic, COLLECTION, PREFIX, TABLE_OVERRIDE, true, true);
        // Add uuid-based connector name
        config.put("name", "8fe3b81d-4b51-43a8-b41c-43e31ecf59c3");
        
        // Cleanup, configure and start connector and sinktask
        String tableName = ConnectorConfigHelper.getDestTable(config, this.gpudb, topic, null);
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector, topic); 

        runSinkTask(task, topic);
        
        // Expect tableName to match override schema+table name combination
        assertEquals(tableName, TABLE_OVERRIDE);
        
        // expect Kinetica table to exist
        boolean tableExists = this.gpudb.hasTable(tableName, null).getTableExists(); 
        assertTrue(tableExists);
        
        // expect table size to match number of Kafka messages generated/ingested
        int size = this.gpudb.showTable(tableName, tableSizeProps).getFullSizes().get(0).intValue();        
        assertEquals(size, TASK_NUM);

        connector.stop();
    }

    @Test
    public void testTopicWithoutSchema() throws Exception {
        String topic = "test.multidot.topic4";
        // Connector is configured to have additional table prefix
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(topic, "", "", null, true, true);
        // Add uuid-based connector name
        config.put("name", "8fe3b81d-4b51-43a8-b41c-43e31ecf59c4");
        
        // Cleanup, configure and start connector and sinktask
        String tableName = ConnectorConfigHelper.getDestTable(config, this.gpudb, topic, null);
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector, topic); 

        runSinkTask(task, topic);
        
        // Expect tableName to match PREFIX+topic name
        String expectedTableName = DEFAULT_COLLECTION + "." + 
                topic.replaceAll("[.]", KineticaSinkConnectorConfig.DEFAULT_DOT_REPLACEMENT);
        
        // expect Kinetica table to exist
        boolean tableExists = this.gpudb.hasTable(expectedTableName, null).getTableExists(); 
        assertTrue(tableExists);
        
        // expect table size to match number of Kafka messages generated/ingested
        int size = this.gpudb.showTable(expectedTableName, tableSizeProps).getFullSizes().get(0).intValue();        
        assertEquals(size, TASK_NUM);

        // cleanup of test tables
        this.tables[3] = expectedTableName;

        connector.stop();
    }

    /**
     * Helper function - starts SinkTask
     * @param connector  KineticaSinkConnector
     * @return           KineticaSinkTask
     */
    private KineticaSinkTask startSinkTask(KineticaSinkConnector connector, String topic) {
        // retrieve taskConfigs from connector
        List<Map<String, String>> taskConfigs = connector.taskConfigs(TASK_NUM);
        Map<String, String> taskConfig = taskConfigs.get(0);
        taskConfig.put(SinkTask.TOPICS_CONFIG, topic);

        // create a new task off taskConfig
        KineticaSinkTask task = new KineticaSinkTask();
        task.start(taskConfig);

        return task;
        
    }
    
    /**
     * Helper function using JsonDataPump test utility
     * @param task         KineticaSinkTask to run 
     * @param topic        Kafka topic name
     * @param messageKey   message key to be used
     * @throws Exception
     */
    private void runSinkTask(KineticaSinkTask task, String topic) throws Exception {
        
        List<SinkRecord> sinkRecords = JsonDataPump.mockSinkRecordJSONTickerMessages(topic);
        task.put(sinkRecords);
        task.flush(null); 
                
        task.stop();
    }        

}
