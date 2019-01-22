package com.kinetica.kafka.utils;

import java.io.FileReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.kinetica.kafka.KineticaSinkConnector;
import com.kinetica.kafka.KineticaSinkConnectorConfig;
import com.kinetica.kafka.SinkSchemaManager;

public class ConnectorConfigHelper {
    private final static Logger LOG = LoggerFactory.getLogger(ConnectorConfigHelper.class);
    private final static String CFG_PATH = "config/quickstart-kinetica-sink.properties";
    
    public static Map<String, String> getBasicConfig() throws Exception {
        
        Map<String, String> cfg = readConfigFile(CFG_PATH);  
        return new HashMap<String, String>((Map) cfg);
    }
    
    public static GPUdb getGPUdb() throws Exception {
        Map<String, String> baseConfig = ConnectorConfigHelper.getBasicConfig();
        try {
            GPUdb gpudb = new GPUdb(baseConfig.get(KineticaSinkConnectorConfig.PARAM_URL), 
                    new GPUdb.Options()
                    .setUsername( baseConfig.get(KineticaSinkConnectorConfig.PARAM_USERNAME))
                    .setPassword( baseConfig.get(KineticaSinkConnectorConfig.PARAM_PASSWORD))
                    .setTimeout(Integer.parseInt( baseConfig.get(KineticaSinkConnectorConfig.PARAM_TIMEOUT))));
            return gpudb;
        }
        catch (GPUdbException ex) {
            ConnectException cex = new ConnectException("Unable to connect to Kinetica at: " + baseConfig.get(KineticaSinkConnectorConfig.PARAM_URL), ex);
            throw cex;
        }
    }
    
    public static Map<String, String> getParameterizedConfig(String topics, String collection,
            String tablePrefix, String tableOverride, boolean createTable, boolean singleTablePerTopic) throws Exception {
        
        Map<String, String> config = getBasicConfig();
        
        config.put(SinkTask.TOPICS_CONFIG, topics);
        config.put(KineticaSinkConnectorConfig.PARAM_COLLECTION, collection);
        config.put(KineticaSinkConnectorConfig.PARAM_TABLE_PREFIX, tablePrefix);
        config.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, tableOverride);
        config.put(KineticaSinkConnectorConfig.PARAM_CREATE_TABLE, Boolean.toString(createTable));
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, Boolean.toString(singleTablePerTopic));
        return config;
    }
    
    public static Map<String, String> getParameterizedConfig(String topics, String collection,
            String tablePrefix, String tableOverride, boolean createTable, boolean allowSchemaEvolution,
            boolean singleTablePerTopic, boolean addNewFields, boolean makeMissingFieldsNullable) throws Exception {
        
        Map<String, String> config = getParameterizedConfig(topics, collection, tablePrefix, tableOverride, createTable, singleTablePerTopic);

        config.put(KineticaSinkConnectorConfig.PARAM_ALLOW_SCHEMA_EVOLUTION, Boolean.toString(allowSchemaEvolution));
        config.put(KineticaSinkConnectorConfig.PARAM_ADD_NEW_FIELDS, Boolean.toString(addNewFields));
        config.put(KineticaSinkConnectorConfig.PARAM_MAKE_MISSING_FIELDS_NULLABLE, Boolean.toString(makeMissingFieldsNullable));
        return config;
    }

    public static Map<String, String> getParameterizedConfig(String topics, String url, String username, String password,
            int timeout, int batchSize, int retryCount, String collection, String tablePrefix, String tableOverride,
            boolean createTable, boolean allowSchemaEvolution, boolean singleTablePerTopic, boolean addNewFields,
            boolean makeMissingFieldsNullable) throws Exception {

        Map<String, String> config = getParameterizedConfig(topics, collection, tablePrefix, tableOverride, createTable, 
                allowSchemaEvolution, singleTablePerTopic, addNewFields, makeMissingFieldsNullable);

        config.put(KineticaSinkConnectorConfig.PARAM_USERNAME, username);
        config.put(KineticaSinkConnectorConfig.PARAM_PASSWORD, password);
        config.put(KineticaSinkConnectorConfig.PARAM_TIMEOUT, ""+timeout);
        config.put(KineticaSinkConnectorConfig.PARAM_BATCH_SIZE, ""+batchSize);
        config.put(KineticaSinkConnectorConfig.PARAM_RETRY_COUNT, ""+retryCount);
        return config;
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Map<String, String> readConfigFile(String fileName) throws Exception {
        Properties props = new Properties();
        try (Reader propsReader = new FileReader(fileName)) {
            props.load(propsReader);
        }
        
        return new HashMap<String, String>((Map) props);
    } 

    /**
     * Helper function - starts Connector
     * @param config     Connector configuration
     * @return           KineticaSinkConnector
     * @throws GPUdbException
     */
    public static KineticaSinkConnector startConnector(Map<String, String> config) throws GPUdbException {
        // Configure and start connector
        KineticaSinkConnector connector = new KineticaSinkConnector();
        connector.start(config);
        
        return connector;
    }
    
    /**
     * Helper function - uses the SinkSchemaManager class to figure out destination table name
     * and cleans up Kinetica table before running test
     * @param config connector config
     * @param topic  Kafka topic name
     * @param schema message schema
     * @return       destination table name
     * @throws Exception
     */
    public static String getDestTable(Map<String, String> config, GPUdb gpudb, String topic, String schema) throws Exception {
        // Runs a Schema Manager on the same config as Connector
        SinkSchemaManager sm = new SinkSchemaManager(config);

        // to use the same function that generates tableName during SinkTask run
        String tableName = sm.getDestTable(topic, schema);
                
        // Kinetica table cleanup before running data ingest
        if(gpudb.hasTable(tableName, null).getTableExists()) {
            gpudb.clearTable(tableName, null, null);
        }
        
        return tableName;
    }
    
    /**
     * Helper function used to cleanup a list of KINETICA tables (either default tablenames, or prefixed with PREFIX)
     * @param prefixed    Flag notifying that table name was manipulated
     * @throws Exception
     */    
    public static void tableCleanUp(GPUdb gpudb, String[] tables) throws GPUdbException {
        for (String tableName : tables) {
            // Kinetica table cleanup before running data ingest
            if(gpudb.hasTable(tableName, null).getTableExists()) {
                gpudb.clearTable(tableName, null, null);
            }
        }
    }

}
