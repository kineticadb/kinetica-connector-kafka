package com.kinetica.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkTask;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.gpudb.GPUdb;
import com.gpudb.RecordObject;


public class SinkSchemaManagerTest {
	
    private GPUdb gpudb;
	
    private Map<String, String> baseSinkConfig() {
        Map<String, String> config = new HashMap<String, String>();
        
        config.put(KineticaSinkConnectorConfig.PARAM_URL, "http://localhost:9191");
        config.put(KineticaSinkConnectorConfig.PARAM_TIMEOUT, "1000");
        config.put("connector.class", "com.kinetica.kafka.KineticaSinkConnector");
        config.put("tasks.max", "1");
        config.put("name", "KineticaQuickStartSinkConnector");
        config.put(KineticaSinkConnectorConfig.PARAM_BATCH_SIZE, "100");
        config.put(KineticaSinkConnectorConfig.PARAM_RETRY_COUNT, "1");
        
        return config;
    }

    private static String replacement = KineticaSinkConnectorConfig.DEFAULT_DOT_REPLACEMENT; 
    
    @Before
    public void setup() throws Exception {
        this.gpudb = TestUtils.getGPUdb();        
    }
    
    @After
    public void cleanup() throws Exception {
    	this.gpudb = null;
    }
    
    
    @Test // #1
    public void noTopicNoCollection() throws Exception {
        String topic_regex = "es_(.)*";
        String inputSchema = "some_schema";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_REGEX_CONFIG, topic_regex);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = "";
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);
        
        // No topic name, no table override, single table per topic flag does not matter, 
        // message is a plain JSON, tableName comes from schema class
        assertEquals(destinationTable, inputSchema);
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }
    
    @Test // #3
    public void singleTopicName() throws Exception {
        String topic = "some_topic"; 
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic;
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);
        
        // Topic name, no collection or prefix, single table per topic
        // tableName comes from topic
        assertEquals(destinationTable, topic);
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }

    
    @Test // #4
    public void multipleTopicNames() throws Exception {
        String topic = "topic1,topic2,topic3"; 
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic.split(",")[0];
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);
        
        // Topic name, no collection or prefix, single table per topic
        // tableName comes from topic
        assertEquals(destinationTable, messageTopic);
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }
    
    @Test // #4.5
    public void singleTopicNameWithDot() throws Exception {
        String topic = "some.topic"; 
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic;
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);
        
        // Topic name, no collection or prefix, single table per topic
        // tableName comes from topic where dots are replaced.
        assertEquals(destinationTable, topic.replaceAll("[.]", replacement));
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }

    @Test // #5
    public void multipleTopicsWithDots() throws Exception {
        String topic = "to.pi.c1,to.pi.c2,to.pi.c3"; 
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic.split(",")[0];
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);
        
        // Topic name, no collection or prefix, single table per topic
        // tableName comes from topic where dots are replaced.
        assertEquals(destinationTable, messageTopic.replaceAll("[.]", replacement));
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }

    @Test // #6
    public void noTopicNoCollectionExistingPrefix() throws Exception {
        String topic_regex = "es_(.)*";
        String prefix = "prefix_";
        String inputSchema = "some_schema";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_REGEX_CONFIG, topic_regex);
        config.put(KineticaSinkConnectorConfig.PARAM_TABLE_PREFIX, prefix);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = "";
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);
        
        // No topic name, no table override, single table per topic flag does not matter, 
        // existing prefix, message is a plain JSON, tableName comes from prefix and schema class
        assertEquals(destinationTable, prefix + inputSchema);
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }
    
    @Test // #8
    public void singleTopicExistingPrefix() throws Exception {
        String topic = "some_topic";
        String prefix = "prefix_";
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_TABLE_PREFIX, prefix);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic;
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);
        
        // Single topic name, no table override, single table per topic, 
        // existing prefix, tableName comes from prefix and topic name
        assertEquals(destinationTable, prefix + topic);
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }

    @Test // #9
    public void multipleTopicsExistingPrefix() throws Exception {
        String topic = "topic1,topic2,topic3";
        String prefix = "prefix_";
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_TABLE_PREFIX, prefix);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic.split(",")[0];
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);
        
        // Multiple topics name, no table override, single table per topic, 
        // existing prefix, tableName comes from prefix and topic name
        assertEquals(destinationTable, prefix + messageTopic);
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }

    @Test // #10
    public void multipleTopicsWithDotsExistingPrefix() throws Exception {
        String topic = "to.pi.c1,to.pi.c2,to.pi.c3";
        String prefix = "prefix_";
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_TABLE_PREFIX, prefix);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic.split(",")[0];
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);
        
        // Multiple topics name, no table override, single table per topic, 
        // existing prefix, tableName comes from prefix and topic name with replaced dots
        assertEquals(destinationTable, prefix + messageTopic.replaceAll("[.]", replacement));
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }

    @Test // #11
    public void noTopicWithCollection() throws Exception {
        String topic_regex = "es_(.)*";
        String inputSchema = "some_schema";
        String collection = "TEST";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_REGEX_CONFIG, topic_regex);
        config.put(KineticaSinkConnectorConfig.PARAM_SCHEMA, collection);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = "";
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);
        
        // No topic name, no table override, single table per topic flag does not matter, 
        // message is a plain JSON, tableName comes from collection and schema class
        assertEquals(destinationTable, collection + "." + inputSchema);
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }
    
    @Test // #13
    public void singleTopicNameWithCollection() throws Exception {
        String topic = "some_topic"; 
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        String collection = "TEST";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        config.put(KineticaSinkConnectorConfig.PARAM_SCHEMA, collection);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic;
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);
        
        // Topic name, no collection or prefix, single table per topic
        // tableName comes from collection and topic
        assertEquals(destinationTable, collection + "." + topic);
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }

    
    @Test // #14
    public void multipleTopicNamesWithCollection() throws Exception {
        String topic = "topic1,topic2,topic3"; 
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        String collection = "TEST";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        config.put(KineticaSinkConnectorConfig.PARAM_SCHEMA, collection);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic.split(",")[0];
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);
        
        // Topic name, no collection or prefix, single table per topic
        // tableName comes from collection and topic
        assertEquals(destinationTable, collection + "." + messageTopic);
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }
    
    @Test // #14.5
    public void singleTopicNameWithDotWithCollection() throws Exception {
        String topic = "some.topic"; 
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        String collection = "TEST";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        config.put(KineticaSinkConnectorConfig.PARAM_SCHEMA, collection);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic;
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);
        
        // Topic name, no collection or prefix, single table per topic
        // tableName comes from collection and topic name where dots are replaced.
        assertEquals(destinationTable, collection + "." + topic.replaceAll("[.]", replacement));
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }

    @Test // #15
    public void multipleTopicsWithDotsWithCollection() throws Exception {
        String topic = "to.pi.c1,to.pi.c2,to.pi.c3"; 
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        String collection = "TEST";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        config.put(KineticaSinkConnectorConfig.PARAM_SCHEMA, collection);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic.split(",")[0];
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);
        
        // Topic name, no collection or prefix, single table per topic
        // tableName comes from collection and topic where dots are replaced.
        assertEquals(destinationTable, collection + "." + messageTopic.replaceAll("[.]", replacement));
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }

    @Test // #16
    public void noTopicExistingPrefixWithCollection() throws Exception {
        String topic_regex = "es_(.)*";
        String prefix = "prefix_";
        String inputSchema = "some_schema";
        String collection = "TEST";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_REGEX_CONFIG, topic_regex);
        config.put(KineticaSinkConnectorConfig.PARAM_TABLE_PREFIX, prefix);
        config.put(KineticaSinkConnectorConfig.PARAM_SCHEMA, collection);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = "";
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);
        
        // No topic name, no table override, single table per topic flag does not matter, 
        // existing prefix, message is a plain JSON, tableName comes from  collection, 
        // prefix and schema class
        assertEquals(destinationTable, collection + "." + prefix + inputSchema);
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }
    
    @Test // #18
    public void singleTopicExistingPrefixWithCollection() throws Exception {
        String topic = "some_topic";
        String prefix = "prefix_";
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        String collection = "TEST";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_TABLE_PREFIX, prefix);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        config.put(KineticaSinkConnectorConfig.PARAM_SCHEMA, collection);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic;
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);
        
        // Single topic name, no table override, single table per topic, 
        // existing prefix, tableName comes from collection, prefix and topic name
        assertEquals(destinationTable, collection + "." + prefix + topic);
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }

    @Test // #19
    public void multipleTopicsExistingPrefixWithCollection() throws Exception {
        String topic = "topic1,topic2,topic3";
        String prefix = "prefix_";
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        String collection = "TEST";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_TABLE_PREFIX, prefix);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        config.put(KineticaSinkConnectorConfig.PARAM_SCHEMA, collection);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic.split(",")[0];
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);
        
        // Multiple topics name, no table override, single table per topic, 
        // existing prefix, tableName comes from collection, prefix and topic name
        assertEquals(destinationTable, collection + "." + prefix + messageTopic);
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }

    @Test // #20
    public void multipleTopicsWithDotsExistingPrefixWithCollection() throws Exception {
        String topic = "to.pi.c1,to.pi.c2,to.pi.c3";
        String prefix = "prefix_";
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        String collection = "TEST";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_TABLE_PREFIX, prefix);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        config.put(KineticaSinkConnectorConfig.PARAM_SCHEMA, collection);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic.split(",")[0];
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);
        
        // Multiple topics name, no table override, single table per topic, 
        // existing prefix, tableName comes from collection, prefix and 
        // topic name with replaced dots
        assertEquals(destinationTable, collection + "." + prefix + messageTopic.replaceAll("[.]", replacement));
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }
    
    @Test // #21
    public void singleTopicWithTableOverride() throws Exception {
        String topic = "some_topic";
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        String override = "tableOverride";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        config.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, override);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic;
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);
        
        // Single topic with table override, single table per topic, 
        // tableName comes from override
        assertEquals(destinationTable, override);
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }

    @Test // #22 
    public void multipleTopicsWithBadTableOverride() throws Exception {
        String topic = "sometopic1,sometopic2,sometopic3";
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        String override = "tableOverride1";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        config.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, override);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic.split(",")[0];
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);
        
        // Multiple topics with table overrides, single table per topic, 
        // tableName comes from override
        assertEquals(destinationTable, messageTopic);
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }

    @Test // #23.5
    public void multipleTopicsAndTableOverridesWithDots() throws Exception {
        String topic = "some.topic1,some.topic2,some.topic3";
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        String override = "Kinetica.DestinationTable1,Kinetica.DestinationTable2,Kinetica.DestinationTable3";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, override);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String destinationTable = schemaMgr.getDestTable(topic.split(",")[0], inputSchema);

        // Multiple topics with table overrides, single table per topic, 
        // tableName comes from override
        assertEquals(destinationTable, override.split(",")[0]);
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }    

    @Test // #23.(n)
    public void multipleTopicWithDotsAndBadTableOverride() throws Exception {
        String topic = "some.topic1,some.topic2,some.topic3";
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        String collection = "TEST";
        String prefix = "prefix_";
        String override = "KineticaDestinationTable1";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, override);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        config.put(KineticaSinkConnectorConfig.PARAM_SCHEMA, collection);
        config.put(KineticaSinkConnectorConfig.PARAM_TABLE_PREFIX, prefix);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic.split(",")[0];
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);

        // In any case where topic is listed and length of the list does not 
        // match the length of the list of table overrides
        // expect override to be ignored, and table name to be derived from 
        // topic name, collection and prefix
        assertEquals(destinationTable, collection + "." + prefix + messageTopic.replaceAll("[.]", replacement));
                
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }    
    
    @Test //#27 
    public void singleTopicWithCollectionAndTableOverride() throws Exception {
        String topic = "some_topic";
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        String collection = "TEST";
        String override = "tableOverride";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, override);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        config.put(KineticaSinkConnectorConfig.PARAM_SCHEMA, collection);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic;
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);

        // For a single topic with collection and table override 
        // table name to be derived from collection and override
        assertEquals(destinationTable, collection + "." + override);
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }     

    @Test //#30
    public void singleTopicWithCollectionPrefixAndTableOverride() throws Exception {
        String topic = "some_topic";
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        String collection = "TEST";
        String prefix = "prefix_";
        String override = "tableOverride";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, override);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        config.put(KineticaSinkConnectorConfig.PARAM_SCHEMA, collection);
        config.put(KineticaSinkConnectorConfig.PARAM_TABLE_PREFIX, prefix);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic;
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);

        // For a single topic with collection, prefix and table override 
        // table name to be derived from collection and override
        assertEquals(destinationTable, collection + "." + override);
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }    

    @Test //#41
    public void multipleTopicsWithDotsollectionAndBadTableOverride() throws Exception {
        String topic = "some.topic1,some.topic2,some.topic3";
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        String collection = "TEST";
        String override = "tableOverride";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, override);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        config.put(KineticaSinkConnectorConfig.PARAM_SCHEMA, collection);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic.split(",")[0];
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);

        // For multiple topics with collection and bad table override 
        // table name to be derived from collection and topic name with replaced dots
        assertEquals(destinationTable, collection + "." + messageTopic.replaceAll("[.]", replacement));
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }     

    @Test //#51
    public void singleTopicWithCollectionAndBadTableOverride() throws Exception {
        String topic = "topic";
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        String collection = "TEST";
        String override = "tableOverride1,tableOverride2,tableOverride3";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, override);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        config.put(KineticaSinkConnectorConfig.PARAM_SCHEMA, collection);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic;
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);

        // For single topic with collection and bad table override 
        // table name to be derived from collection and topic name with replaced dots
        assertEquals(destinationTable, collection + "." + messageTopic);
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }     

    @Test //#61
    public void multipleTopicsWithPrefixAndTableOverridesWithDots() throws Exception {
        String topic = "topic1,topic2,topic3";
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        String prefix = "prefix_";
        String override = "table.Override1,table.Override2,table.Override3";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, override);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        config.put(KineticaSinkConnectorConfig.PARAM_TABLE_PREFIX, prefix);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic.split(",")[0];
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);

        // For multiple topics with prefix and good table override  (with dots)
        // table name to be derived from override
        assertEquals(destinationTable, override.split(",")[0]);
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    }     

    @Test //#64
    public void multipleTopicsWithCollectionAndTableOverride() throws Exception {
        String topic = "topic1,topic2,topic3";
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        String collection = "TEST";
        String override = "tableOverride1,tableOverride2,tableOverride3";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, override);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        config.put(KineticaSinkConnectorConfig.PARAM_SCHEMA, collection);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic.split(",")[0];
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);

        // For multiple topics with collection and good table override (no dots) 
        // table name to be derived from collection and override
        assertEquals(destinationTable, collection + "." + override.split(",")[0]);
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    } 

    @Test //#68
    public void multipleTopicsWithDotsCollectionPrefixAndTableOverride() throws Exception {
        String topic = "to.pi.c1,to.pi.c2,to.pi.c3";
        String inputSchema = "some_schema";
        String singleTablePerTopic = "true";
        String collection = "TEST";
        String prefix = "prefix_";
        String override = "table.Override1,table.Override2,table.Override3";
        
        Map<String, String> config  = baseSinkConfig();
        config.put(SinkTask.TOPICS_CONFIG, topic);
        config.put(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE, override);
        config.put(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC, singleTablePerTopic);
        config.put(KineticaSinkConnectorConfig.PARAM_SCHEMA, collection);
        config.put(KineticaSinkConnectorConfig.PARAM_TABLE_PREFIX, prefix);
        
        SinkSchemaManager schemaMgr = new SinkSchemaManager(config);
        String messageTopic = topic.split(",")[0];
        String destinationTable = schemaMgr.getDestTable(messageTopic, inputSchema);

        // For multiple topics with collection and good table override (with dots)
        // table name to be derived from override
        assertEquals(destinationTable, override.split(",")[0]);
        
        TestUtils.tableCleanUp(this.gpudb, destinationTable);
    } 
}
