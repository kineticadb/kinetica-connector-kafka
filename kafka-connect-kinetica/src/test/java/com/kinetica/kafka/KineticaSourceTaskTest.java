package com.kinetica.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase;
import com.gpudb.RecordObject;
import com.gpudb.protocol.CreateTableRequest;

public class KineticaSourceTaskTest {
    private final static Logger LOG = LoggerFactory.getLogger(KineticaSourceTaskTest.class);

    private final static String TOPIC = "topic";
    private final static String TABLE = "table";
    private final static String COLLECTION = "TEST";
    private final static int PARTITION = 0;
    private static final Schema SCHEMA = SchemaBuilder.struct()
            .name("com.kinetica.kafka.TweetRecord")
            .field("x", Schema.FLOAT32_SCHEMA)
            .field("y", Schema.FLOAT32_SCHEMA)
            .field("timestamp", Schema.INT64_SCHEMA)
            .field("TEXT", Schema.STRING_SCHEMA)
            .field("AUTHOR", Schema.STRING_SCHEMA)
            .field("URL", Schema.STRING_SCHEMA)
            .field("test_int", Schema.INT32_SCHEMA)
            .field("test_long", Schema.INT64_SCHEMA)
            .field("test_double", Schema.FLOAT64_SCHEMA)
            .field("test_bytes", Schema.BYTES_SCHEMA)
            .field("test_schema_reg", Schema.STRING_SCHEMA)
            .build();
    
    private Map<String, String> config = null;
    
    @Before
    public void setup() throws Exception {
        this.config = getConfig("config/quickstart-kinetica-source.properties");
        
        String gpudbURL = config.get(KineticaSourceConnectorConfig.PARAM_URL);
        String tableName = config.get(KineticaSourceConnectorConfig.PARAM_TABLE_NAMES);
        GPUdb gpudb = new GPUdb(gpudbURL, new GPUdb.Options()
                .setUsername(config.get(KineticaSourceConnectorConfig.PARAM_USERNAME))
                .setPassword(config.get(KineticaSourceConnectorConfig.PARAM_PASSWORD))
                .setTimeout(0));
        if (gpudb.hasTable(tableName, null).getTableExists()) {
            LOG.info("Dropping table: {}", tableName);
            gpudb.clearTable(tableName, null, null);
        }

        String typeId = RecordObject.createType(TweetRecord.class, gpudb);

        LOG.info("Creating table: {}", tableName);
        Map<String, String> options = 
        		GPUdbBase.options(CreateTableRequest.Options.COLLECTION_NAME, COLLECTION);
        gpudb.createTable(tableName, typeId, options);

    }
     
    @Test
    public void startPollStopTaskTest() throws InterruptedException {
        KineticaSourceConnector sourceConnector = new KineticaSourceConnector();
        sourceConnector.start(this.config);

        List<Map<String, String>> taskConfigs = sourceConnector.taskConfigs(1);
        Map<String, String> taskConfig = taskConfigs.get(0);
        
        KineticaSourceTask task = new KineticaSourceTask();
        task.start(taskConfig);
        assertNotNull(task);
        assertNotNull(task.getZmqUrl());
        assertNotNull(task.getQueue());
        assertEquals(task.getTopicPrefix(), taskConfig.get(KineticaSourceConnectorConfig.PARAM_TOPIC_PREFIX));
        assertEquals(task.version(), AppInfoParser.getVersion());
        
        
        for (SourceRecord record : generateRecords(100)) {
            task.getQueue().put(record);
        }
        Thread.sleep(1000);
        
        if(! task.getQueue().isEmpty()) {
            task.poll();
            task.getQueue().clear();
        }
        
        task.stop();
        sourceConnector.stop();
    }
  

    /**
     * Helper function
     * Generates a given number of SourceRecords of key-value HashMap
     * @param number  number of records to generate
     * @return Collection of SinkRecords
     */
    private Collection<SourceRecord> generateRecords(int number) {
        List<SourceRecord> sourceRecords = new ArrayList<SourceRecord>();
        TweetRecord data = null;
        
        for (int i = 0; i < number; i++) {
            data = TweetRecord.generateRandomRecord();            
            sourceRecords.add(new SourceRecord(this.config, this.config, TOPIC, PARTITION, Schema.STRING_SCHEMA, null, SCHEMA, data, data.timestamp));
        }
            
        return sourceRecords;
    }


    /**
     * Helper function
     * Loads configuration from provided property file
     * @param fileName  property file
     * @return          a map of properties
     * @throws Exception
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Map<String, String> getConfig(String fileName) throws Exception {
        Properties props = new Properties();
        try (Reader propsReader = new FileReader(fileName)) {
            LOG.info("Loading properties: {}", fileName);
            props.load(propsReader);
        }

        return new HashMap<String, String>((Map) props);
    } 
}