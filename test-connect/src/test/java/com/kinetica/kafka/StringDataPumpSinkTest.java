package com.kinetica.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.kinetica.kafka.utils.ConnectorConfigHelper;

public class StringDataPumpSinkTest {
    private final static Logger LOG = LoggerFactory.getLogger(StringDataPumpSinkTest.class);
    private final static String TOPIC = "Apple_topic";
    private final static String TABLE = "Apple_table";
    private final static String OGG_TOPIC = "Ogg_Apple_topic";
    private final static String OGG_TABLE = "Ogg_Apple_table";
    private final static String COLLECTION = "TEST";
    private final static String PREFIX = "out";
    
    private final static int batch_size = 10;
    private final static int batch_count = 10;
    
    private final static int TASK_NUM = 1;
    
    private HashMap<String, String> tableSizeProps;
    
    private GPUdb gpudb;
    
    @Before
    public void setup() throws Exception {
        this.gpudb = ConnectorConfigHelper.getGPUdb();
        this.tableSizeProps = new HashMap<String, String>();
        this.tableSizeProps.put("get_sizes", "true");
        ConnectorConfigHelper.tableCleanUp(this.gpudb, new String[] {TOPIC, PREFIX+TOPIC, TABLE, OGG_TOPIC, PREFIX+OGG_TOPIC, OGG_TABLE });
    }
    
    @After
    public void cleanup() throws GPUdbException {
        this.gpudb = null;
    }
        
    // Non-OGG Json message means that Kafka record has null for a key and Json map of k/v pairs for payload 
    // No Kinetica table renaming or prefixing
    @Test
    public void testSchemalessNonOGGJson() throws Exception {
        // Connector is configured to have no additional table prefix and no tablename override
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(TOPIC, COLLECTION, "", "", true, true);
        
        // Cleanup, configure and start connector and sinktask
        String tableName = ConnectorConfigHelper.getDestTable(config, gpudb, TOPIC, null);
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 

        // pass nulls for Kafka message keys
        runSinkTask(task, TOPIC, null);
        
        // Expect tableName to match TOPIC name
        assertEquals(tableName, TOPIC);
        
        // expect Kinetica table to exist
        boolean tableExists = gpudb.hasTable(tableName, null).getTableExists(); 
        assertTrue(tableExists);
        
        // expect table size to match number of Kafka messages generated/ingested
        int size = gpudb.showTable(tableName, tableSizeProps).getFullSizes().get(0).intValue();        
        assertEquals(size, batch_size*batch_count);

        connector.stop();

    }

    // Non-OGG Json message means that Kafka record has null for a key and Json map of k/v pairs for payload 
    // Kinetica table is prefixed
    @Test
    public void testSchemalessNonOGGJsonWithPrefix() throws Exception {
        // Connector is configured to have a tablename prefix, no tablename override
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(TOPIC, COLLECTION, PREFIX, "", true, true);
        
        // Cleanup, configure and start connector and sinktask
        String tableName = ConnectorConfigHelper.getDestTable(config, gpudb, TOPIC, null);
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 

        // pass nulls for Kafka message keys
        runSinkTask(task, TOPIC, null);
        
        // Expect tableName to match PREFIX+TOPIC name
        assertEquals(tableName, PREFIX+TOPIC);
        
        // expect Kinetica table to exist
        boolean tableExists = gpudb.hasTable(tableName, null).getTableExists(); 
        assertTrue(tableExists);
        
        // expect table size to match number of Kafka messages generated/ingested

        int size = gpudb.showTable(tableName, tableSizeProps).getFullSizes().get(0).intValue();        
        assertEquals(size, batch_size*batch_count);

        connector.stop();

    }

    // Non-OGG Json message means that Kafka record has null for a key and Json map of k/v pairs for payload 
    // Kinetica table is renamed
    @Test
    public void testSchemalessNonOGGJsonWithOverride() throws Exception {
        // Connector is configured to have a tablename override
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(TOPIC, COLLECTION, "", TABLE, true, true);
        
        // Cleanup, configure and start connector and sinktask
        String tableName = ConnectorConfigHelper.getDestTable(config, gpudb, TOPIC, null);
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 


        // pass nulls for Kafka message keys
        runSinkTask(task, TOPIC, null);
        
        // Expect tableName to match PREFIX+TOPIC name
        assertEquals(tableName, TABLE);
        
        // expect Kinetica table to exist
        boolean tableExists = gpudb.hasTable(tableName, null).getTableExists(); 
        assertTrue(tableExists);
        
        // expect table size to match number of Kafka messages generated/ingested
        int size = gpudb.showTable(tableName, tableSizeProps).getFullSizes().get(0).intValue();        
        assertEquals(size, batch_size*batch_count);

        connector.stop();

    }

    // OGG Json message means that Kafka record has table name for key and Json map of k/v pairs for payload 
    // No Kinetica table renaming or prefixing
    @Test
    public void testSchemalessOGGJson() throws Exception {
        // Connector is configured to have no additional table prefix and no tablename override
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(OGG_TOPIC, COLLECTION, "", "", true, true);
        
        // Cleanup, configure and start connector and sinktask
        String tableName = ConnectorConfigHelper.getDestTable(config, gpudb, OGG_TOPIC, OGG_TABLE);
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 

        // pass tableName for Kafka message keys
        runSinkTask(task, OGG_TOPIC, OGG_TABLE);
        
        // Expect tableName to match OGG_TOPIC name
        assertEquals(tableName, OGG_TOPIC);
        
        // expect Kinetica table to exist
        boolean tableExists = gpudb.hasTable(tableName, null).getTableExists(); 
        assertTrue(tableExists);
        
        // expect table size to match number of Kafka messages generated/ingested
        int size = gpudb.showTable(tableName, tableSizeProps).getFullSizes().get(0).intValue();        
        assertEquals(size, batch_size*batch_count);

        connector.stop();

    }

    // OGG Json message means that Kafka record has table name for key and Json map of k/v pairs for payload
    // Kinetica table is prefixed
    @Test
    public void testSchemalessOGGJsonWithPrefix() throws Exception {
        // Connector is configured to have no additional table prefix and no tablename override
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(OGG_TOPIC, COLLECTION, PREFIX, "", true, true);
        
        // Cleanup, configure and start connector and sinktask
        String tableName = ConnectorConfigHelper.getDestTable(config, gpudb, OGG_TOPIC, OGG_TABLE);
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 

        // pass tableName for Kafka message keys
        runSinkTask(task, OGG_TOPIC, OGG_TABLE);
        
        // Expect tableName to match PREFIX+OGG_TOPIC name
        assertEquals(tableName, PREFIX+OGG_TOPIC);
        
        // expect Kinetica table to exist
        boolean tableExists = gpudb.hasTable(tableName, null).getTableExists(); 
        assertTrue(tableExists);
        
        // expect table size to match number of Kafka messages generated/ingested
        int size = gpudb.showTable(tableName, tableSizeProps).getFullSizes().get(0).intValue();        
        assertEquals(size, batch_size*batch_count);

        connector.stop();

    }

    // OGG Json message means that Kafka record has table name for key and Json map of k/v pairs for payload
    // No Kinetica table is renamed
    @Test
    public void testSchemalessOGGJsonWithOverride() throws Exception {
        // Connector is configured to have no additional table prefix and no tablename override
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(OGG_TOPIC, COLLECTION, "", OGG_TABLE, true, true);
        
        // Cleanup, configure and start connector and sinktask
        String tableName = ConnectorConfigHelper.getDestTable(config, gpudb, OGG_TOPIC, OGG_TABLE);
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 

        // pass tableName for Kafka message keys
        runSinkTask(task, OGG_TOPIC, OGG_TABLE);
        
        // Expect tableName to match OGG_TABLE name
        assertEquals(tableName, OGG_TABLE);
        
        // expect Kinetica table to exist
        boolean tableExists = gpudb.hasTable(tableName, null).getTableExists(); 
        assertTrue(tableExists);
        
        // expect table size to match number of Kafka messages generated/ingested
        int size = gpudb.showTable(tableName, tableSizeProps).getFullSizes().get(0).intValue();        
        assertEquals(size, batch_size*batch_count);

        connector.stop();

    }

    /**
     * Helper function - starts SinkTask
     * @param connector  KineticaSinkConnector
     * @return           KineticaSinkTask
     */
    private KineticaSinkTask startSinkTask(KineticaSinkConnector connector) {
        // retrieve taskConfigs from connector
        List<Map<String, String>> taskConfigs = connector.taskConfigs(TASK_NUM);
        Map<String, String> taskConfig = taskConfigs.get(0);
        taskConfig.put(SinkTask.TOPICS_CONFIG, OGG_TOPIC);

        // create a new task off taskConfig
        KineticaSinkTask task = new KineticaSinkTask();
        task.start(taskConfig);

        return task;
        
    }

    /**
     * Helper function using StringDataPump test utility
     * @param task         KineticaSinkTask to run 
     * @param topic        Kafka topic name
     * @param messageKey   message key to be used
     * @throws Exception
     */
    private void runSinkTask(KineticaSinkTask task, String topic, String messageKey) throws Exception {
        
        List<SinkRecord> sinkRecords = StringDataPump.mockSinkRecordJSONMessages(topic, messageKey);
    
        task.put(sinkRecords);
        task.flush(null); 
                
        task.stop();
    }

}
