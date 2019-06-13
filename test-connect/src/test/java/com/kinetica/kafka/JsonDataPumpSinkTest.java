package com.kinetica.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpudb.GPUdb;
import com.kinetica.kafka.data.utils.KafkaSchemaHelpers;
import com.kinetica.kafka.utils.ConnectorConfigHelper;

public class JsonDataPumpSinkTest {
    private final static Logger LOG = LoggerFactory.getLogger(StringDataPumpSinkTest.class);
    private final static String TOPIC = "fruits";
    private final static String AVRO_TOPIC = "avrofruits";
    private final static String COLLECTION = "TEST";
    private final static String PREFIX = "out";
    private final static String AVRO_PREFIX = "avro";
    private final static String NULL_SUFFIX = "_with_nulls";
        
    private final static int TASK_NUM = 1;
    
    private HashMap<String, String> tableSizeProps;
    
    private GPUdb gpudb;
    
    @Before
    public void setup() throws Exception {
        this.gpudb = ConnectorConfigHelper.getGPUdb();
        this.tableSizeProps = new HashMap<String, String>();
        this.tableSizeProps.put("get_sizes", "true");
    }
    
    @After
    public void cleanup() throws Exception{
    	List<String> tableNames = new ArrayList<String>();
    	for (String fruit : KafkaSchemaHelpers.FRUITS) {
    		tableNames.add(fruit);
    		tableNames.add(AVRO_PREFIX + fruit);
    		tableNames.add(PREFIX + fruit);
    		tableNames.add(fruit + NULL_SUFFIX);
    	}
    	ConnectorConfigHelper.tableCleanUp(this.gpudb, tableNames.toArray(new String[tableNames.size()]));
        this.gpudb = null;
    }
        
    // OGG Json message means that Kafka record has Table name for a key and Json map of k/v pairs for payload 
    // No Kinetica table renaming or prefixing
    @Test
    public void testSchemalessOGGJson() throws Exception {
        // Connector is configured to have no additional table prefix and no tablename override
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(TOPIC, COLLECTION, "", "", true, false);
        
        // Cleanup, configure and start connector and sinktask
        ConnectorConfigHelper.tableCleanUp(this.gpudb, KafkaSchemaHelpers.FRUITS);
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 
        
        // run data generation
        runSinkTask(task, TOPIC, false, false);
        Thread.sleep(1000);
        
        // expect Kinetica tables to exist and be populated
        for (String tableName : KafkaSchemaHelpers.FRUITS) {
            boolean tableExists = gpudb.hasTable(tableName, null).getTableExists(); 
            assertTrue(tableExists);
            
            // expect table size to match number of Kafka messages generated/ingested
            int size = gpudb.showTable(tableName, tableSizeProps).getFullSizes().get(0).intValue();        
            assertEquals(size, JsonDataPump.batchSize*JsonDataPump.count);
        }
        Thread.sleep(2000);
        connector.stop();

    }
    
    
	// OGG Json message means that Kafka record has Table name for a key and Json map of k/v pairs for payload 
	// No Kinetica table renaming or prefixing
	@Test
	public void testSchemalessOGGJsonWithNulls() throws Exception {
		String topic = TOPIC + "WithNulls";
	    // Connector is configured to have no additional table prefix and no tablename override
	    Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(topic, COLLECTION, "", "", true, false);
	    
	    // Cleanup, configure and start connector and sinktask
	    ConnectorConfigHelper.tableCleanUp(this.gpudb, KafkaSchemaHelpers.FRUITS);
	    KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
	    KineticaSinkTask task = startSinkTask(connector); 
	    
	    // run data generation
	    runSinkTask(task, topic, false, true);
	    Thread.sleep(1000);
	    
	    // expect Kinetica tables to exist and be populated
	    for (String fruit : KafkaSchemaHelpers.FRUITS) {
	    	String tableName = fruit + NULL_SUFFIX;
	        boolean tableExists = gpudb.hasTable(tableName, null).getTableExists(); 
	        assertTrue(tableExists);
	        
	        // expect table size to match number of Kafka messages generated/ingested
	        int size = gpudb.showTable(tableName, tableSizeProps).getFullSizes().get(0).intValue();        
	        assertEquals(size, JsonDataPump.batchSize*JsonDataPump.count);
	    }
	    Thread.sleep(2000);
	    connector.stop();
	
	}

    // OGG Json message means that Kafka record has Table name for a key and Json map of k/v pairs for payload 
    // Kinetica table is prefixed
    @Test
    public void testSchemalessOGGJsonWithPrefix() throws Exception {
        // Connector is configured to have additional table prefix
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(TOPIC, COLLECTION, PREFIX, "", true, false);
        
        // Cleanup, configure and start connector and sinktask
        String[] tables = new String[KafkaSchemaHelpers.FRUITS.length];
        for (int i = 0; i < tables.length; i++) {
            // Kinetica table cleanup before running data ingest
            tables[i] = PREFIX + KafkaSchemaHelpers.FRUITS[i];
        }
        ConnectorConfigHelper.tableCleanUp(this.gpudb, tables);
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 

        // run data generation
        runSinkTask(task, TOPIC, false, false);
        Thread.sleep(2000);
        
        // expect Kinetica tables to exist and be populated
        for (String tableName : KafkaSchemaHelpers.FRUITS) {

            boolean tableExists = gpudb.hasTable(PREFIX + tableName, null).getTableExists(); 
            assertTrue(tableExists);

            // expect table size to match number of Kafka messages generated/ingested
            int size = gpudb.showTable(PREFIX + tableName, tableSizeProps).getFullSizes().get(0).intValue();        
            assertEquals(size, JsonDataPump.batchSize*JsonDataPump.count);
        }
        Thread.sleep(2000);
        connector.stop();

    }

    // Avro encoded Json messages 
    // Kinetica table with prefix
    @Test
    public void testAvroSchemaJson() throws Exception {
        // Connector is configured to have additional table prefix
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(TOPIC, COLLECTION, AVRO_PREFIX, "", true, false);
        
        // Cleanup, configure and start connector and sinktask
        String[] tables = new String[KafkaSchemaHelpers.FRUITS.length];
        for (int i = 0; i < tables.length; i++) {
            // Kinetica table cleanup before running data ingest
            tables[i] = AVRO_PREFIX + KafkaSchemaHelpers.FRUITS[i];
        }
        ConnectorConfigHelper.tableCleanUp(this.gpudb, tables);
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 

        // run data generation
        runSinkTask(task, AVRO_TOPIC, true, false);
        Thread.sleep(1000);
        
        // expect Kinetica tables to exist and be populated
        for (String tableName : KafkaSchemaHelpers.FRUITS) {
            boolean tableExists = gpudb.hasTable(AVRO_PREFIX+tableName, null).getTableExists(); 
            assertTrue(tableExists);
            
            // expect table size to match number of Kafka messages generated/ingested
            int size = gpudb.showTable(AVRO_PREFIX+tableName, tableSizeProps).getFullSizes().get(0).intValue();        
            assertEquals(size, JsonDataPump.batchSize*JsonDataPump.count);
        }
        Thread.sleep(2000);
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
        taskConfig.put(SinkTask.TOPICS_CONFIG, TOPIC);

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
    private void runSinkTask(KineticaSinkTask task, String topic, boolean avroEncoded, boolean withNulls) throws Exception {
        
        List<SinkRecord> sinkRecords;
        
        if (withNulls) {
        	sinkRecords =  JsonDataPump.mockSinkRecordJSONMessagesWithNulls(topic);
        } else if (avroEncoded) {
            sinkRecords =  JsonDataPump.mockSinkRecordAvroMessages(topic);
        } else {
            sinkRecords =  JsonDataPump.mockSinkRecordJSONMessages(topic);
        }

        task.put(sinkRecords);
        task.flush(null); 
                
        task.stop();
    }

}
