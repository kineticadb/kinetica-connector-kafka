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
import org.junit.Test;

import com.gpudb.GPUdb;
import com.gpudb.protocol.ShowTableResponse;
import com.kinetica.kafka.data.utils.SchemaRegistryUtils;
import com.kinetica.kafka.utils.ConnectorConfigHelper;
import org.apache.kafka.common.KafkaException;

public class SchemaTestSerializerDataPumpSinkTest {

    private final static String TOPIC = "SchemaVersion";
    private final static String COLLECTION = "TEST";
    private final static int batch_size = 10;
    private final static int TASK_NUM = 1;
    
    private HashMap<String, String> tableSizeProps;
    
    private GPUdb gpudb;

    
    @Before
    public void setup() throws Exception {
        this.gpudb = ConnectorConfigHelper.getGPUdb();
        this.tableSizeProps = new HashMap<String, String>();
        this.tableSizeProps.put("get_sizes", "true");
        String[] tables = new String[6];
        for (int i = 0; i < 6; i++) {
            tables[i] = "test" + (i+1) + TOPIC;
        }
        ConnectorConfigHelper.tableCleanUp(this.gpudb, tables);
    }
    
    @After
    public void cleanup() {
        this.gpudb = null;
    }
    

    // Kafka connector supports schema evolution,  
    // does not allow to add columns to Kinetica table or convert columns for missing data to nullable
    // Table prefix used 
    @Test 
    public void testAvroSchemaEvolutionSchema1to2() throws Exception {
        // Connector is configured to have an additional table prefix
        String prefix = "test1";
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(TOPIC, COLLECTION, prefix, "", true, true, true, false, false, true);

        // Configure and start connector and sinktask
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 

        // run data generation
        runSinkTask(task, TOPIC, new int[] {1, 2});
        Thread.sleep(1000);
        
        boolean tableExists = gpudb.hasTable(prefix+TOPIC, null).getTableExists(); 
        assertTrue(tableExists);

        // expect table size to match number of Kafka messages generated/ingested
        ShowTableResponse response = gpudb.showTable(prefix+TOPIC, tableSizeProps);        
        int size = response.getFullSizes().get(0).intValue();
        assertEquals(size, batch_size*2);
        
        Map<String, List<String>> props = response.getProperties().get(0);
        int columnCount = props.size();
        int controlCount = SchemaRegistryUtils.getKafkaSchemaByVersion(1).fields().size();
        assertEquals(columnCount, controlCount);
    }

    // Kafka connector supports schema evolution,  
    // does not allow to add columns to Kinetica table or convert columns for missing data to nullable
    // Table prefix used 
    @Test 
    public void testAvroSchemaEvolutionSchema1to3() throws Exception {
        // Connector is configured to have an additional table prefix
        String prefix = "test2";
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(TOPIC, COLLECTION, prefix, "", true, true, true, false, false, true);

        // Configure and start connector and sinktask
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 

        // run data generation
        runSinkTask(task, TOPIC, new int[] {1, 2});
        Thread.sleep(1000);
        
        boolean tableExists = gpudb.hasTable(prefix+TOPIC, null).getTableExists(); 
        assertTrue(tableExists);

        // expect table size to match number of Kafka messages generated/ingested
        ShowTableResponse response = gpudb.showTable(prefix+TOPIC, tableSizeProps);        
        int size = response.getFullSizes().get(0).intValue();
        assertEquals(size, batch_size*2);
        
        Map<String, List<String>> props = response.getProperties().get(0);
        int columnCount = props.size();
        int controlCount = SchemaRegistryUtils.getKafkaSchemaByVersion(1).fields().size();
        assertEquals(columnCount, controlCount);
    }

    // Kafka connector supports schema evolution,  
    // does not allow to add columns to Kinetica table or convert columns for missing data to nullable
    // Table prefix used 
    @Test 
    public void testAvroSchemaEvolutionSchema3to1() throws Exception {
        // Connector is configured to have an additional table prefix
        String prefix = "test3";
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(TOPIC, COLLECTION, prefix, "", true, true, true, false, false, true);

        // Configure and start connector and sinktask
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 

        // run data generation
        runSinkTask(task, TOPIC, new int[] {3, 1});
        Thread.sleep(1000);
        
        boolean tableExists = gpudb.hasTable(prefix+TOPIC, null).getTableExists(); 
        assertTrue(tableExists);

        // expect table size to match number of Kafka messages generated/ingested
        ShowTableResponse response = gpudb.showTable(prefix+TOPIC, tableSizeProps);        
        int size = response.getFullSizes().get(0).intValue();
        assertEquals(size, batch_size*2);
        
        Map<String, List<String>> props = response.getProperties().get(0);
        int columnCount = props.size();
        int controlCount = SchemaRegistryUtils.getKafkaSchemaByVersion(3).fields().size();
        assertEquals(columnCount, controlCount);
    }

    // Kafka connector supports schema evolution,  
    // does not allow to add columns to Kinetica table or convert columns for missing data to nullable
    // Table prefix used 
    @Test 
    public void testAvroSchemaEvolutionSchema1to4() throws Exception {
        // Connector is configured to have an additional table prefix
        String prefix = "test4";
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(TOPIC, COLLECTION, prefix, "", true, true, true, false, false, true);

        // Configure and start connector and sinktask
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 

        // run data generation
        runSinkTask(task, TOPIC, new int[] {1, 4});
        Thread.sleep(1000);
        
        boolean tableExists = gpudb.hasTable(prefix+TOPIC, null).getTableExists(); 
        assertTrue(tableExists);

        // expect table size to match number of Kafka messages generated/ingested
        ShowTableResponse response = gpudb.showTable(prefix+TOPIC, tableSizeProps);        
        int size = response.getFullSizes().get(0).intValue();
        assertEquals(size, batch_size*2);
        
        Map<String, List<String>> props = response.getProperties().get(0);
        int columnCount = props.size();
        int controlCount = SchemaRegistryUtils.getKafkaSchemaByVersion(1).fields().size();
        assertEquals(columnCount, controlCount);
    }

    // Kafka connector supports schema evolution,  
    // does not allow to add columns to Kinetica table or convert columns for missing data to nullable
    // Table prefix used 
    @Test 
    public void testAvroSchemaEvolutionSchema4to3() throws Exception {
        // Connector is configured to have an additional table prefix
        String prefix = "test5";
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(TOPIC, COLLECTION, prefix, "", true, true, true, false, false, true);

        // Configure and start connector and sinktask
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 

        // run data generation
        runSinkTask(task, TOPIC, new int[] {4, 3});
        Thread.sleep(1000);
        
        boolean tableExists = gpudb.hasTable(prefix+TOPIC, null).getTableExists(); 
        assertTrue(tableExists);

        // expect table size to match number of Kafka messages generated/ingested
        ShowTableResponse response = gpudb.showTable(prefix+TOPIC, tableSizeProps);        
        int size = response.getFullSizes().get(0).intValue();
        assertEquals(size, batch_size*2);
        
        Map<String, List<String>> props = response.getProperties().get(0);
        int columnCount = props.size();
        int controlCount = SchemaRegistryUtils.getKafkaSchemaByVersion(4).fields().size();
        assertEquals(columnCount, controlCount);
    }

    // Kafka connector supports schema evolution, but due to wrong schema version error, no evolution would happen  
    // does not allow to add columns to Kinetica table or convert columns for missing data to nullable
    // Table prefix used 
    @Test (expected = KafkaException.class)
    public void testAvroSchemaEvolutionSchema4to0() throws Exception {
        // Connector is configured to have an additional table prefix
        String prefix = "test6";
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(TOPIC, COLLECTION, prefix, "", true, true, true, false, false, true);

        // Configure and start connector and sinktask
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 

        // run data generation
        runSinkTask(task, TOPIC, new int[] {4, 0});
        Thread.sleep(1000);
        
        boolean tableExists = gpudb.hasTable(prefix+TOPIC, null).getTableExists(); 
        assertTrue(tableExists);

        // expect table size to match number of Kafka messages generated/ingested
        ShowTableResponse response = gpudb.showTable(prefix+TOPIC, tableSizeProps);        
        int size = response.getFullSizes().get(0).intValue();
        assertEquals(size, batch_size);
        
        Map<String, List<String>> props = response.getProperties().get(0);
        int columnCount = props.size();
        int controlCount = SchemaRegistryUtils.getKafkaSchemaByVersion(4).fields().size();
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
            sinkRecords = SchemaTestSerializerDataPump.mockAvroSerialized(topic, version, batch_size);
            task.put(sinkRecords);
        }
        
        Thread.sleep(1000);
        task.flush(null); 
                
        task.stop();
    }

}
