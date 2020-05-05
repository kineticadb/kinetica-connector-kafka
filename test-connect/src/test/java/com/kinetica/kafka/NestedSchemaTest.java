package com.kinetica.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase;
import com.gpudb.protocol.ClearTableRequest;
import com.gpudb.protocol.ShowTableResponse;
import com.kinetica.kafka.data.utils.SchemaRegistryUtils;
import com.kinetica.kafka.utils.ConnectorConfigHelper;

public class NestedSchemaTest {

    private final static String NESTED_TYPE_TOPIC = "NestedTypeTopic";
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
    }
    
    @After
    public void cleanup() {
        this.gpudb = null;
    }
    
    
    // Kafka connector is configured to insert all records of complex type flattening field names with '.' join 
    @Test
    public void testNestedTypeFlattening() throws Exception {
        
        String tableName = "temp_agg_table";
        Map<String, String> config = 
                ConnectorConfigHelper.getParameterizedConfig(NESTED_TYPE_TOPIC, COLLECTION, "", "", true, false, true, false, false, false);
        // Connector is configured to flatten source schema
        config.put(KineticaSinkConnectorConfig.PARAM_FLATTEN_SOURCE_SCHEMA, "true");
        config.put(KineticaSinkConnectorConfig.PARAM_ARRAY_VALUE_SEPARATOR, ",");
        config.put(KineticaSinkConnectorConfig.PARAM_FIELD_NAME_DELIMITER, "_");
        config.put(
                KineticaSinkConnectorConfig.PARAM_ARRAY_FLATTENING_MODE, 
                KineticaSinkConnectorConfig.ARRAY_FLATTENING.CONVERT_TO_STRING.name());
        // Connector is configured to have destination table override
        config.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, tableName);
        
        gpudb.clearTable(tableName, "", 
                GPUdbBase.options(ClearTableRequest.Options.NO_ERROR_IF_NOT_EXISTS, ClearTableRequest.Options.TRUE));

        // Configure and start connector and sinktask
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector, NESTED_TYPE_TOPIC, ""); 
        
        // run data generation
        runSinkTask(task, SchemaRegistryUtils.KAFKA_ES_GEO1_NESTED_SCHEMA.name(), NESTED_TYPE_TOPIC);
        runSinkTask(task, SchemaRegistryUtils.KAFKA_ES_GEO1_NESTED_SCHEMA.name(), NESTED_TYPE_TOPIC);
        runSinkTask(task, SchemaRegistryUtils.KAFKA_ES_GEO1_NESTED_SCHEMA.name(), NESTED_TYPE_TOPIC);
        Thread.sleep(1000);
        
        boolean tableExists = gpudb.hasTable(tableName, null).getTableExists(); 
        assertTrue(tableExists);
        
        // expect table size to match number of Kafka messages generated/ingested
        ShowTableResponse response = gpudb.showTable(tableName, tableSizeProps);        
        int size = response.getFullSizes().get(0).intValue();
        // Kinetica table has 3 batches of data stored because 3 batches of data were sent  
        // and PK is not set on data table 
        assertEquals(size, 3 * batch_size);
        
    } 
      
    // Kafka connector is configured to insert all records of complex type flattening field names with '.' join  
    @Test
    public void testNestedTypeFlatteningTopicsRegex() throws Exception {
        
        String tableName = "temp_agg_table_regex";            
        this.gpudb.clearTable(tableName, "", 
                GPUdbBase.options(ClearTableRequest.Options.NO_ERROR_IF_NOT_EXISTS, ClearTableRequest.Options.TRUE));

        // Connector is configured to have an additional table prefix
        String prefix = "es_starterkit_";
        Map<String, String> config = 
                ConnectorConfigHelper.getParameterizedConfig(prefix, COLLECTION, "", "", true, false, false, false, false, false);
        // Replace topics subscription with topics.regex
        config.put(SinkTask.TOPICS_CONFIG, "");
        config.put(SinkTask.TOPICS_REGEX_CONFIG, prefix+"*");
        
        config.put(KineticaSinkConnectorConfig.PARAM_FLATTEN_SOURCE_SCHEMA, "true");
        config.put(KineticaSinkConnectorConfig.PARAM_ARRAY_VALUE_SEPARATOR, ",");
        config.put(
                KineticaSinkConnectorConfig.PARAM_ARRAY_FLATTENING_MODE, 
                KineticaSinkConnectorConfig.ARRAY_FLATTENING.CONVERT_TO_STRING.name());
        config.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, tableName);

        // Configure and start connector and sinktask
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = this.startSinkTask(connector, "", prefix+"*"); 

        for (int topic_id=1; topic_id<=3; topic_id++) {
            String topicName = prefix + NESTED_TYPE_TOPIC + "_0" + topic_id;
            
            // run data generation
            this.runSinkTask(task, SchemaRegistryUtils.KAFKA_ES_GEO1_NESTED_SCHEMA.name(), topicName);
        }

        Thread.sleep(1000);
        
        boolean tableExists = this.gpudb.hasTable(tableName, null).getTableExists(); 
        assertTrue(tableExists);
        
        // expect table size to match number of Kafka messages generated/ingested
        ShowTableResponse response = this.gpudb.showTable(tableName, this.tableSizeProps);        
        int size = response.getFullSizes().get(0).intValue();
        // Kinetica table has 3 batches of data stored because 3 batches of data were sent  
        // through 3 different topics
        assertEquals(size, 3 * batch_size);
        
    }  
    
    // Kafka connector is configured to insert all records of complex type flattening field names with '.' join  
    @Test
    public void testNestedTypeArrayFlatteningTopicsRegex() throws Exception {
      
        String tableName = "temp_agg_arr_table";            
        this.gpudb.clearTable(tableName, "", 
            GPUdbBase.options(ClearTableRequest.Options.NO_ERROR_IF_NOT_EXISTS, ClearTableRequest.Options.TRUE));

        // Connector is configured to have an additional table prefix
        String prefix = "es_starterkit_";
        Map<String, String> config = 
            ConnectorConfigHelper.getParameterizedConfig(prefix, COLLECTION, "", "", true, false, false, false, false, false);
        // Replace topics subscription with topics.regex
        config.put(SinkTask.TOPICS_CONFIG, "");
        config.put(SinkTask.TOPICS_REGEX_CONFIG, prefix+"*");
      
        config.put(KineticaSinkConnectorConfig.PARAM_FLATTEN_SOURCE_SCHEMA, "true");
        config.put(KineticaSinkConnectorConfig.PARAM_ARRAY_VALUE_SEPARATOR, ",");
        config.put(
              KineticaSinkConnectorConfig.PARAM_ARRAY_FLATTENING_MODE, 
              KineticaSinkConnectorConfig.ARRAY_FLATTENING.CONVERT_TO_STRING.name());
        config.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, tableName);

        // Configure and start connector and sinktask
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = this.startSinkTask(connector, "", prefix+"*"); 

        for (int topic_id=1; topic_id<=3; topic_id++) {
            String topicName = prefix + NESTED_TYPE_TOPIC + "_0" + topic_id;
          
            // run data generation
            this.runSinkTask(task, SchemaRegistryUtils.KAFKA_ES_GEO2_NESTED_SCHEMA.name(), topicName);
        }

        Thread.sleep(1000);
      
        boolean tableExists = this.gpudb.hasTable(tableName, null).getTableExists(); 
        assertTrue(tableExists);
      
        // expect table size to match number of Kafka messages generated/ingested
        ShowTableResponse response = this.gpudb.showTable(tableName, this.tableSizeProps);        
        int size = response.getFullSizes().get(0).intValue();
        // Kinetica table has 3 batches of data stored because 3 batches of data were sent  
        // through 3 different topics
        assertEquals(size, 3 * batch_size);
      
    }  

    // Kafka connector is configured to insert all records of complex type flattening field names with '.' join  
    @Test
    public void testNestedTypeFlatteningCase_1() throws Exception {
      
      String tableName = "table_case1";            
      Map<String, String> config = 
              ConnectorConfigHelper.getParameterizedConfig(NESTED_TYPE_TOPIC, COLLECTION, "", "", true, false, true, false, false, false);
      // Connector is configured to flatten source schema
      config.put(KineticaSinkConnectorConfig.PARAM_FLATTEN_SOURCE_SCHEMA, "true");
      config.put(KineticaSinkConnectorConfig.PARAM_ARRAY_VALUE_SEPARATOR, ",");
      config.put(KineticaSinkConnectorConfig.PARAM_FIELD_NAME_DELIMITER, "_");
      config.put(
              KineticaSinkConnectorConfig.PARAM_ARRAY_FLATTENING_MODE, 
              KineticaSinkConnectorConfig.ARRAY_FLATTENING.CONVERT_TO_STRING.name());
      // Connector is configured to have destination table override
      config.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, tableName);
      
      gpudb.clearTable(tableName, "", 
              GPUdbBase.options(ClearTableRequest.Options.NO_ERROR_IF_NOT_EXISTS, ClearTableRequest.Options.TRUE));

      // Configure and start connector and sinktask
      KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
      KineticaSinkTask task = startSinkTask(connector, NESTED_TYPE_TOPIC, ""); 
      
      // run data generation
      runSinkTask(task, SchemaRegistryUtils.KAFKA_CASE1_SCHEMA.name(), NESTED_TYPE_TOPIC);
      Thread.sleep(1000);
      
      boolean tableExists = gpudb.hasTable(tableName, null).getTableExists(); 
      assertTrue(tableExists);
      
      // expect table size to match number of Kafka messages generated/ingested
      ShowTableResponse response = gpudb.showTable(tableName, tableSizeProps);        
      int size = response.getFullSizes().get(0).intValue();
      // Kinetica table has 3 batches of data stored because 3 batches of data were sent  
      // and PK is not set on data table 
      assertEquals(size, batch_size);
      
    }  
    
    // Kafka connector is configured to insert all records of complex type flattening field names with '.' join  
    @Test
    public void testNestedTypeFlatteningCase_2() throws Exception {
      
      String tableName = "table_case2";            
      Map<String, String> config = 
              ConnectorConfigHelper.getParameterizedConfig(NESTED_TYPE_TOPIC, COLLECTION, "", "", true, false, true, false, false, false);
      // Connector is configured to flatten source schema
      config.put(KineticaSinkConnectorConfig.PARAM_FLATTEN_SOURCE_SCHEMA, "true");
      config.put(KineticaSinkConnectorConfig.PARAM_ARRAY_VALUE_SEPARATOR, ",");
      config.put(KineticaSinkConnectorConfig.PARAM_FIELD_NAME_DELIMITER, "_");
      config.put(
              KineticaSinkConnectorConfig.PARAM_ARRAY_FLATTENING_MODE, 
              KineticaSinkConnectorConfig.ARRAY_FLATTENING.CONVERT_TO_STRING.name());
      // Connector is configured to have destination table override
      config.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, tableName);
      
      gpudb.clearTable(tableName, "", 
              GPUdbBase.options(ClearTableRequest.Options.NO_ERROR_IF_NOT_EXISTS, ClearTableRequest.Options.TRUE));

      // Configure and start connector and sinktask
      KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
      KineticaSinkTask task = startSinkTask(connector, NESTED_TYPE_TOPIC, ""); 
      
      // run data generation
      runSinkTask(task, SchemaRegistryUtils.KAFKA_CASE2_SCHEMA.name(), NESTED_TYPE_TOPIC);
      Thread.sleep(1000);
      
      boolean tableExists = gpudb.hasTable(tableName, null).getTableExists(); 
      assertTrue(tableExists);
      
      // expect table size to match number of Kafka messages generated/ingested
      ShowTableResponse response = gpudb.showTable(tableName, tableSizeProps);        
      int size = response.getFullSizes().get(0).intValue();
      // Kinetica table has 3 batches of data stored because 3 batches of data were sent  
      // and PK is not set on data table 
      assertEquals(size, batch_size);
      
  }  
  
    /**
     * Helper function - starts SinkTask
     * @param connector  KineticaSinkConnector
     * @return           KineticaSinkTask
     */
    private KineticaSinkTask startSinkTask(KineticaSinkConnector connector, String topic, String regex) {
        // retrieve taskConfigs from connector
        List<Map<String, String>> taskConfigs = connector.taskConfigs(TASK_NUM);

        Map<String, String> taskConfig = taskConfigs.get(0);

        // create a new task off taskConfig
        KineticaSinkTask task = new KineticaSinkTask();
        if (!topic.isEmpty())
            taskConfig.put(SinkTask.TOPICS_CONFIG, topic);
        else 
            taskConfig.put(SinkTask.TOPICS_REGEX_CONFIG, regex);
        task.start(taskConfig);

        return task;
        
    }
    
    /**
     * Helper function using AvroSerializerDataPump test utility
     * @param task         KineticaSinkTask to run 
     * @param topic        Kafka topic name
     * @throws Exception
     */
    private HashMap<String, SinkRecord> runSinkTask(KineticaSinkTask task, String schemaName, String topic) throws Exception {
                
        List<SinkRecord> sinkRecords;
        
        sinkRecords = SchemaTestSerializerDataPump.mockNestedValues(topic, schemaName, batch_size);
        for (SinkRecord rec : sinkRecords) {
            System.out.println(rec);
        }
        task.put(sinkRecords);
        
        Thread.sleep(1000);
        task.flush(null);
                
        task.stop();
        
        // return a hashmap of sink records for value comparison, using symbol PK as key
        HashMap<String, SinkRecord> result = new HashMap<String, SinkRecord>();
        for (SinkRecord rec : sinkRecords) {
            if (rec.valueSchema().type() == org.apache.kafka.connect.data.Schema.Type.STRUCT) {
                Struct val = (Struct)rec.value();
                String key;
                if (schemaName.equals("es_geoip1")) {
                    key = "property1";
                } else if (schemaName.equals("es_geoip2")){
                    key = "property1";
                } else if (schemaName.equals("case1")){
                    key = "property_ts";
                } else if (schemaName.equals("case2")){
                    key = "top_object_id";
                } else {
                    key = "lastUpdated";
                }

                result.put(val.getString(key), rec);
            }
        }
        return result;
    }
    
}
