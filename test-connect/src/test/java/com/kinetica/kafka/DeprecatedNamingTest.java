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
import com.kinetica.kafka.utils.ConnectorConfigHelper;

public class DeprecatedNamingTest {
	private GPUdb gpudb;
	private HashMap<String, String> tableSizeProps;
	private String[] tables = {
			"deprecated_collection_name.deprecated_dest_table_override", 
			"actual_schema_name.actual_destination_name", 
			"actual_schema_name.actual_prefixtopic3"};
	
    
    public static final String PARAM_URL_VALUE         = "http://127.0.0.1:9191";
    public static final String PARAM_USERNAME_VALUE    = "";
    public static final String PARAM_PASSWORD_VALUE    = "";
    public static final String PARAM_TIMEOUT_VALUE     = "0";
    public static final String PARAM_RETRY_COUNT_VALUE = "3";
    public static final String PARAM_BATCH_SIZE_VALUE  = "1";
    
    
    public static final String DEPRECATED_PARAM_COLLECTION_VALUE                   = "deprecated_collection_name";
    public static final String DEPRECATED_PARAM_DEST_TABLE_OVERRIDE_VALUE          = "deprecated_dest_table_override";
    public static final String DEPRECATED_PARAM_TABLE_PREFIX_VALUE                 = "deprecated_table_prefix";
    public static final String DEPRECATED_PARAM_CREATE_TABLE_VALUE                 = "true";
    public static final String DEPRECATED_PARAM_SINGLE_TABLE_PER_TOPIC_VALUE       = "true";
    public static final String DEPRECATED_PARAM_UPDATE_ON_EXISTING_PK_VALUE        = "true";

    public static final String DEPRECATED_PARAM_ALLOW_SCHEMA_EVOLUTION_VALUE       = "false";
    public static final String DEPRECATED_PARAM_ADD_NEW_FIELDS_VALUE               = "false"; 
    public static final String DEPRECATED_PARAM_MAKE_MISSING_FIELDS_NULLABLE_VALUE = "false";
	
    
    public static final String PARAM_TABLE_PREFIX_VALUE                 = "actual_prefix";
    public static final String PARAM_SCHEMA_VALUE                       = "actual_schema_name";
    public static final String PARAM_DEST_TABLE_OVERRIDE_VALUE          = "actual_destination_name";
    public static final String PARAM_CREATE_TABLE_VALUE                 = "true";
    public static final String PARAM_SINGLE_TABLE_PER_TOPIC_VALUE       = "false";
    public static final String PARAM_UPDATE_ON_EXISTING_PK_VALUE        = "false";

    public static final String PARAM_ALLOW_SCHEMA_EVOLUTION_VALUE       = "true";
    public static final String PARAM_ADD_NEW_FIELDS_VALUE               = "true";
    public static final String PARAM_MAKE_MISSING_FIELDS_NULLABLE_VALUE = "true";
    
    
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
    public void testTopicWithDeprecatedParameters() throws Exception {
        String topic = "topic1";
        
        Map<String, String> config = new HashMap<String, String>();
        // Add uuid-based connector name
        config.put("name", "KineticaConnectorName");
        config.put(SinkTask.TOPICS_CONFIG, topic);

        
        config.put(KineticaSinkConnectorConfig.PARAM_URL, PARAM_URL_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_USERNAME, PARAM_USERNAME_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_PASSWORD, PARAM_PASSWORD_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_TIMEOUT, PARAM_TIMEOUT_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_RETRY_COUNT, PARAM_RETRY_COUNT_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_BATCH_SIZE, PARAM_BATCH_SIZE_VALUE);
        
        config.put(KineticaSinkConnectorConfig.DEPRECATED_PARAM_COLLECTION, DEPRECATED_PARAM_COLLECTION_VALUE);
        config.put(KineticaSinkConnectorConfig.DEPRECATED_PARAM_DEST_TABLE_OVERRIDE, DEPRECATED_PARAM_DEST_TABLE_OVERRIDE_VALUE);
        config.put(KineticaSinkConnectorConfig.DEPRECATED_PARAM_TABLE_PREFIX, DEPRECATED_PARAM_TABLE_PREFIX_VALUE);
        config.put(KineticaSinkConnectorConfig.DEPRECATED_PARAM_CREATE_TABLE, DEPRECATED_PARAM_CREATE_TABLE_VALUE);
        config.put(KineticaSinkConnectorConfig.DEPRECATED_PARAM_SINGLE_TABLE_PER_TOPIC, DEPRECATED_PARAM_SINGLE_TABLE_PER_TOPIC_VALUE);
        config.put(KineticaSinkConnectorConfig.DEPRECATED_PARAM_UPDATE_ON_EXISTING_PK, DEPRECATED_PARAM_UPDATE_ON_EXISTING_PK_VALUE);
        config.put(KineticaSinkConnectorConfig.DEPRECATED_PARAM_ALLOW_SCHEMA_EVOLUTION, DEPRECATED_PARAM_ALLOW_SCHEMA_EVOLUTION_VALUE);
        config.put(KineticaSinkConnectorConfig.DEPRECATED_PARAM_ADD_NEW_FIELDS, DEPRECATED_PARAM_ADD_NEW_FIELDS_VALUE);
        config.put(KineticaSinkConnectorConfig.DEPRECATED_PARAM_MAKE_MISSING_FIELDS_NULLABLE, DEPRECATED_PARAM_MAKE_MISSING_FIELDS_NULLABLE_VALUE);
        
        // Cleanup, configure and start connector and sinktask
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector, topic); 

        runSinkTask(task, topic);

        assertEquals(task.schemaMgr.collectionName, DEPRECATED_PARAM_COLLECTION_VALUE);
        assertEquals(task.schemaMgr.tableOverride, DEPRECATED_PARAM_DEST_TABLE_OVERRIDE_VALUE);
        assertEquals(task.schemaMgr.tablePrefix, DEPRECATED_PARAM_TABLE_PREFIX_VALUE);
        assertEquals(task.schemaMgr.createTable, Boolean.parseBoolean(DEPRECATED_PARAM_CREATE_TABLE_VALUE));
        assertEquals(task.schemaMgr.singleTablePerTopic, Boolean.parseBoolean(DEPRECATED_PARAM_SINGLE_TABLE_PER_TOPIC_VALUE));
        assertEquals(task.schemaMgr.updateOnExistingPK, Boolean.parseBoolean(DEPRECATED_PARAM_UPDATE_ON_EXISTING_PK_VALUE));
        assertEquals(task.schemaMgr.allowSchemaEvolution, Boolean.parseBoolean(DEPRECATED_PARAM_ALLOW_SCHEMA_EVOLUTION_VALUE));
        assertEquals(task.schemaMgr.addNewColumns, Boolean.parseBoolean(DEPRECATED_PARAM_ADD_NEW_FIELDS_VALUE));
        assertEquals(task.schemaMgr.alterColumnsToNullable, Boolean.parseBoolean(DEPRECATED_PARAM_MAKE_MISSING_FIELDS_NULLABLE_VALUE));
        
        
        connector.stop();
    }
    
    @Test
    public void testTopicWithDeprecatedAndActualParameters() throws Exception {
        String topic = "topic2";
        
        Map<String, String> config = new HashMap<String, String>();
        // Add uuid-based connector name
        config.put("name", "KineticaConnectorName");
        config.put(SinkTask.TOPICS_CONFIG, topic);

        
        config.put(KineticaSinkConnectorConfig.PARAM_URL, PARAM_URL_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_USERNAME, PARAM_USERNAME_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_PASSWORD, PARAM_PASSWORD_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_TIMEOUT, PARAM_TIMEOUT_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_RETRY_COUNT, PARAM_RETRY_COUNT_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_BATCH_SIZE, PARAM_BATCH_SIZE_VALUE);
        
        config.put(KineticaSinkConnectorConfig.DEPRECATED_PARAM_COLLECTION, DEPRECATED_PARAM_COLLECTION_VALUE);
        config.put(KineticaSinkConnectorConfig.DEPRECATED_PARAM_DEST_TABLE_OVERRIDE, DEPRECATED_PARAM_DEST_TABLE_OVERRIDE_VALUE);
        config.put(KineticaSinkConnectorConfig.DEPRECATED_PARAM_TABLE_PREFIX, DEPRECATED_PARAM_TABLE_PREFIX_VALUE);
        config.put(KineticaSinkConnectorConfig.DEPRECATED_PARAM_CREATE_TABLE, "false");
        config.put(KineticaSinkConnectorConfig.DEPRECATED_PARAM_SINGLE_TABLE_PER_TOPIC, DEPRECATED_PARAM_SINGLE_TABLE_PER_TOPIC_VALUE);
        config.put(KineticaSinkConnectorConfig.DEPRECATED_PARAM_UPDATE_ON_EXISTING_PK, DEPRECATED_PARAM_UPDATE_ON_EXISTING_PK_VALUE);
        config.put(KineticaSinkConnectorConfig.DEPRECATED_PARAM_ALLOW_SCHEMA_EVOLUTION, DEPRECATED_PARAM_ALLOW_SCHEMA_EVOLUTION_VALUE);
        config.put(KineticaSinkConnectorConfig.DEPRECATED_PARAM_ADD_NEW_FIELDS, DEPRECATED_PARAM_ADD_NEW_FIELDS_VALUE);
        config.put(KineticaSinkConnectorConfig.DEPRECATED_PARAM_MAKE_MISSING_FIELDS_NULLABLE, DEPRECATED_PARAM_MAKE_MISSING_FIELDS_NULLABLE_VALUE);
        
        config.put(KineticaSinkConnectorConfig.PARAM_SCHEMA, PARAM_SCHEMA_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, PARAM_DEST_TABLE_OVERRIDE_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_TABLE_PREFIX, PARAM_TABLE_PREFIX_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_CREATE_TABLE, PARAM_CREATE_TABLE_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, PARAM_SINGLE_TABLE_PER_TOPIC_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_UPDATE_ON_EXISTING_PK, PARAM_UPDATE_ON_EXISTING_PK_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_ALLOW_SCHEMA_EVOLUTION, PARAM_ALLOW_SCHEMA_EVOLUTION_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_ADD_NEW_FIELDS, PARAM_ADD_NEW_FIELDS_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_MAKE_MISSING_FIELDS_NULLABLE, PARAM_MAKE_MISSING_FIELDS_NULLABLE_VALUE);
        
        // Cleanup, configure and start connector and sinktask
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector, topic); 

        runSinkTask(task, topic);
        
        assertEquals(task.schemaMgr.collectionName, PARAM_SCHEMA_VALUE);
        assertEquals(task.schemaMgr.tableOverride, PARAM_DEST_TABLE_OVERRIDE_VALUE);
        assertEquals(task.schemaMgr.tablePrefix, PARAM_TABLE_PREFIX_VALUE);
        assertEquals(task.schemaMgr.createTable, Boolean.parseBoolean(PARAM_CREATE_TABLE_VALUE));
        assertEquals(task.schemaMgr.singleTablePerTopic, Boolean.parseBoolean(PARAM_SINGLE_TABLE_PER_TOPIC_VALUE));
        assertEquals(task.schemaMgr.updateOnExistingPK, Boolean.parseBoolean(PARAM_UPDATE_ON_EXISTING_PK_VALUE));
        assertEquals(task.schemaMgr.allowSchemaEvolution, Boolean.parseBoolean(PARAM_ALLOW_SCHEMA_EVOLUTION_VALUE));
        assertEquals(task.schemaMgr.addNewColumns, Boolean.parseBoolean(PARAM_ADD_NEW_FIELDS_VALUE));
        assertEquals(task.schemaMgr.alterColumnsToNullable, Boolean.parseBoolean(PARAM_MAKE_MISSING_FIELDS_NULLABLE_VALUE));
        
        
        connector.stop();
    }

    @Test
    public void testTopicWithActualParameters() throws Exception {
        String topic = "topic3";
        
        Map<String, String> config = new HashMap<String, String>();
        // Add uuid-based connector name
        config.put("name", "KineticaConnectorName");
        config.put(SinkTask.TOPICS_CONFIG, topic);

        
        config.put(KineticaSinkConnectorConfig.PARAM_URL, PARAM_URL_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_USERNAME, PARAM_USERNAME_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_PASSWORD, PARAM_PASSWORD_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_TIMEOUT, PARAM_TIMEOUT_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_RETRY_COUNT, PARAM_RETRY_COUNT_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_BATCH_SIZE, PARAM_BATCH_SIZE_VALUE);
        
        config.put(KineticaSinkConnectorConfig.PARAM_SCHEMA, PARAM_SCHEMA_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, "");
        config.put(KineticaSinkConnectorConfig.PARAM_TABLE_PREFIX, PARAM_TABLE_PREFIX_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_CREATE_TABLE, PARAM_CREATE_TABLE_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, PARAM_SINGLE_TABLE_PER_TOPIC_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_UPDATE_ON_EXISTING_PK, PARAM_UPDATE_ON_EXISTING_PK_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_ALLOW_SCHEMA_EVOLUTION, PARAM_ALLOW_SCHEMA_EVOLUTION_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_ADD_NEW_FIELDS, PARAM_ADD_NEW_FIELDS_VALUE);
        config.put(KineticaSinkConnectorConfig.PARAM_MAKE_MISSING_FIELDS_NULLABLE, PARAM_MAKE_MISSING_FIELDS_NULLABLE_VALUE);
        
        // Cleanup, configure and start connector and sinktask
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector, topic); 

        runSinkTask(task, topic);
        
        assertEquals(task.schemaMgr.collectionName, PARAM_SCHEMA_VALUE);
        assertEquals(task.schemaMgr.tableOverride, "");
        assertEquals(task.schemaMgr.tablePrefix, PARAM_TABLE_PREFIX_VALUE);
        assertEquals(task.schemaMgr.createTable, Boolean.parseBoolean(PARAM_CREATE_TABLE_VALUE));
        assertEquals(task.schemaMgr.singleTablePerTopic, Boolean.parseBoolean(PARAM_SINGLE_TABLE_PER_TOPIC_VALUE));
        assertEquals(task.schemaMgr.updateOnExistingPK, Boolean.parseBoolean(PARAM_UPDATE_ON_EXISTING_PK_VALUE));
        assertEquals(task.schemaMgr.allowSchemaEvolution, Boolean.parseBoolean(PARAM_ALLOW_SCHEMA_EVOLUTION_VALUE));
        assertEquals(task.schemaMgr.addNewColumns, Boolean.parseBoolean(PARAM_ADD_NEW_FIELDS_VALUE));
        assertEquals(task.schemaMgr.alterColumnsToNullable, Boolean.parseBoolean(PARAM_MAKE_MISSING_FIELDS_NULLABLE_VALUE));
        
        connector.stop();
    }
    
    
    
    /**
     * Helper function - starts SinkTask
     * @param connector  KineticaSinkConnector
     * @return           KineticaSinkTask
     */
    private KineticaSinkTask startSinkTask(KineticaSinkConnector connector, String topic) {
        // retrieve taskConfigs from connector
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
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
