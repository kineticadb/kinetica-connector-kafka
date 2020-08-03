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

import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.protocol.ShowTableResponse;
import com.kinetica.kafka.data.utils.SchemaRegistryUtils;
import com.kinetica.kafka.utils.ConnectorConfigHelper;

public class AvroSerializerDataPumpSinkTest {
    
    private final static String TOPIC = "SchemaVersion";
    private final static String TABLE = "SchemaTable";
    private final static String COLLECTION = "TEST";
    private final static String PREFIX = "out";
    private final static int batch_size = 10;
    private final static int TASK_NUM = 1;
    
    private HashMap<String, String> tableSizeProps;
    
    private GPUdb gpudb;

    
    @Before
    public void setup() throws Exception {
        this.gpudb = ConnectorConfigHelper.getGPUdb();
        this.tableSizeProps = new HashMap<String, String>();
        this.tableSizeProps.put("get_sizes", "true");
        ConnectorConfigHelper.tableCleanUp(this.gpudb, 
            ConnectorConfigHelper.addCollection(
                new String[] {TOPIC, TABLE, PREFIX+TOPIC, "noEdits", "ForwardCompatibility", "BackwardCompatibility"}, 
                COLLECTION));
    }
    
    @After
    public void cleanup() throws GPUdbException {
    	ConnectorConfigHelper.tableCleanUp(this.gpudb, 
                ConnectorConfigHelper.addCollection(
                    new String[] {TOPIC, TABLE, PREFIX+TOPIC, "noEdits", "ForwardCompatibility", "BackwardCompatibility"}, 
                    COLLECTION));
        this.gpudb = null;
    }
    
    // Kafka connector supports schema evolution,  
    // allows to add columns to Kinetica table and convert columns for missing data to nullable
    // No table prefix or table override 
    @Test 
    public void testAvroSchemaEvolutionFullCompatibility() throws Exception {
        // Connector is configured to have no additional table prefix
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(TOPIC, COLLECTION, "", "", true, true, true, true, true, true);

        // Configure and start connector and sinktask
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 

        // run data generation
        runSinkTask(task, TOPIC, new int[] {1, 2, 3, 4});
        Thread.sleep(1000);
        
        boolean tableExists = gpudb.hasTable(ConnectorConfigHelper.addCollection(TOPIC, COLLECTION), null).getTableExists(); 
        assertTrue(tableExists);

        // expect table size to match number of Kafka messages generated/ingested
        ShowTableResponse response = gpudb.showTable(ConnectorConfigHelper.addCollection(TOPIC, COLLECTION), tableSizeProps);
        int size = response.getFullSizes().get(0).intValue();
        assertEquals(size, batch_size*4);
        
        Map<String, List<String>> props = response.getProperties().get(0);
        int columnCount = props.size();
        int controlCount = SchemaRegistryUtils.getKafkaSchemaByVersion(4).fields().size();
        assertEquals(columnCount, controlCount);
    }

    // Kafka connector supports schema evolution,  
    // allows to add columns to Kinetica table and convert columns for missing data to nullable
    // Table name prefix is used, no table override 
    @Test
    public void testAvroSchemaEvolutionWithTablePrefix() throws Exception {
        // Connector is configured to have additional table prefix
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(TOPIC, COLLECTION, PREFIX, "", true, true, true, true, true, true);
        // Cleanup, configure and start connector and sinktask
        
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 

        // run data generation
        runSinkTask(task, TOPIC, new int[] {1, 2, 3, 4});
        Thread.sleep(1000);
        
        boolean tableExists = gpudb.hasTable(ConnectorConfigHelper.addCollection(PREFIX+TOPIC, COLLECTION), null).getTableExists(); 
        assertTrue(tableExists);

        // expect table size to match number of Kafka messages generated/ingested
        int size = gpudb.showTable(ConnectorConfigHelper.addCollection(PREFIX+TOPIC, COLLECTION), tableSizeProps).getFullSizes().get(0).intValue();        
        assertEquals(size, batch_size*4);

    }

    // Kafka connector supports schema evolution,  
    // allows to add columns to Kinetica table and convert columns for missing data to nullable
    // Table name override 
    @Test 
    public void testAvroSchemaEvolutionWithTableOverride() throws Exception {
        // Connector is configured to have tablename override
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(TOPIC, COLLECTION, "", TABLE, true, true, true, true, true, true);
        // Cleanup, configure and start connector and sinktask
        
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 

        // run data generation
        runSinkTask(task, TOPIC, new int[] {1, 2, 3, 4});
        Thread.sleep(1000);
        
        boolean tableExists = gpudb.hasTable(ConnectorConfigHelper.addCollection(TABLE, COLLECTION), null).getTableExists(); 
        assertTrue(tableExists);

        // expect table size to match number of Kafka messages generated/ingested
        int size = gpudb.showTable(ConnectorConfigHelper.addCollection(TABLE, COLLECTION), tableSizeProps).getFullSizes().get(0).intValue();        
        assertEquals(size, batch_size*4);

    }

    // Kafka connector supports schema evolution,  
    // Does not allow to add columns to Kinetica table or convert columns for missing data to nullable, extra fields are ignored
    // Table override is used 
    @Test 
    public void testAvroSchemaEvolutionWithNoTableEditing() throws Exception {
        // Connector is configured to override table name
        String tableOverride = "noEdits";
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(TOPIC, COLLECTION, "", tableOverride, true, true, true, false, false, true);
        // Cleanup, configure and start connector and sinktask
        
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 

        // run data generation
        runSinkTask(task, TOPIC, new int[] {1, 2, 3, 4});
        Thread.sleep(1000);
        
        boolean tableExists = gpudb.hasTable(ConnectorConfigHelper.addCollection(tableOverride, COLLECTION), null).getTableExists(); 
        assertTrue(tableExists);

        // expect table size to match number of Kafka messages generated/ingested        
        ShowTableResponse response = gpudb.showTable(ConnectorConfigHelper.addCollection(tableOverride, COLLECTION), tableSizeProps);

        int size = response.getFullSizes().get(0).intValue();
        assertEquals(size, batch_size*4);

        Map<String, List<String>> props = response.getProperties().get(0);
        int columnCount = props.size();
        int controlCount = SchemaRegistryUtils.getKafkaSchemaByVersion(1).fields().size();
        assertEquals(columnCount, controlCount);

    }

    // Kafka connector supports schema evolution with forward compatibility,  
    // allows to add columns to Kinetica table, but not convert columns for missing data to nullable, faulty data is ignored
    // Table override is used 
    @Test 
    public void testAvroSchemaEvolutionWithForwardCompatibility() throws Exception {
        // Connector is configured to override table name
        String tableOverride = "ForwardCompatibility";
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(TOPIC, COLLECTION, "", tableOverride, true, true, true, true, false, true);
        // Cleanup, configure and start connector and sinktask
        
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 

        // run data generation
        runSinkTask(task, TOPIC, new int[] {1, 2, 4});
        Thread.sleep(1000);
        
        boolean tableExists = gpudb.hasTable(ConnectorConfigHelper.addCollection(tableOverride, COLLECTION), null).getTableExists(); 
        assertTrue(tableExists);

        // expect table size to match number of Kafka messages generated/ingested        
        ShowTableResponse response = gpudb.showTable(ConnectorConfigHelper.addCollection(tableOverride, COLLECTION), tableSizeProps);

        int size = response.getFullSizes().get(0).intValue();
        assertEquals(size, batch_size*3);

        Map<String, List<String>> props = response.getProperties().get(0);
        int columnCount = props.size();
        int controlCount = SchemaRegistryUtils.getKafkaSchemaByVersion(4).fields().size();
        assertEquals(columnCount, controlCount);

    }
    
    // Kafka connector supports schema evolution with backward compatibility,  
    // Does not allow to add columns to Kinetica table, allows to convert columns for missing data to nullable, extra fields are ignored
    // Table override is used 
    @Test 
    public void testAvroSchemaEvolutionWithBackwardCompatibility() throws Exception {
        // Connector is configured to override table name
        String tableOverride = "BackwardCompatibility";
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(TOPIC, COLLECTION, "", tableOverride, true, true, true, false, true, true);
        // Cleanup, configure and start connector and sinktask
        
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 

        // run data generation
        runSinkTask(task, TOPIC, new int[] {3, 1, 2, 4});
        Thread.sleep(1000);
        
        boolean tableExists = gpudb.hasTable(ConnectorConfigHelper.addCollection(tableOverride, COLLECTION), null).getTableExists(); 
        assertTrue(tableExists);

        // expect table size to match number of Kafka messages generated/ingested        
        ShowTableResponse response = gpudb.showTable(ConnectorConfigHelper.addCollection(tableOverride, COLLECTION), tableSizeProps);

        int size = response.getFullSizes().get(0).intValue();
        assertEquals(size, batch_size*4);

        Map<String, List<String>> props = response.getProperties().get(0);
        int columnCount = props.size();
        int controlCount = SchemaRegistryUtils.getKafkaSchemaByVersion(3).fields().size();
        assertEquals(columnCount, controlCount);

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
     * Helper function using AvroSerializerDataPump test utility
     * @param task         KineticaSinkTask to run 
     * @param topic        Kafka topic name
     * @throws Exception
     */
    private void runSinkTask(KineticaSinkTask task, String topic, int[] versions) throws Exception {
                
        List<SinkRecord> sinkRecords;
        
        for (int version : versions) {
            sinkRecords = AvroSerializerDataPump.mockAvroSerialized(topic, version, batch_size);
            task.put(sinkRecords);
        }
        
        Thread.sleep(1000);
        task.flush(null); 
                
        task.stop();
    }

}
