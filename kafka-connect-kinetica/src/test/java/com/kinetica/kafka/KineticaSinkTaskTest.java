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
import java.util.Random;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpudb.Type.Column;

public class KineticaSinkTaskTest {
    
    private final static Logger LOG = LoggerFactory.getLogger(KineticaSinkTaskTest.class);
    private final static String TOPIC = "topic";
    private final static String TABLE = "table";
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
    private final Random rnd = new Random();
    
    private Map<String, String> config = null;
    
    @Before
    public void setup() throws Exception {
        this.config = TestConnector.getConfig("config/quickstart-kinetica-sink.properties");
        
    }

    @Test
    public void startPutFlushJSONTest() {
        KineticaSinkTask task = new KineticaSinkTask();
        assertNotNull(task);
        task.start(this.config);
        assertEquals(task.version(), AppInfoParser.getVersion());
        
        
        Collection<SinkRecord> jsonRecords = generateJSON(3);
        
        task.put(jsonRecords);
        
        task.flush(null);
        
        task.stop();      
    }
    

    @Test
    public void booleanToIntConversionJSONinput() throws Exception {
        KineticaSinkTask task = new KineticaSinkTask();
        assertNotNull(task);
        task.start(this.config);        

        String topic = "json_topic_with_boolean";
        String tableName = "json_table_with_boolean";
        
        // Create random fake sinkRecords
    	List<SinkRecord> sinkRecords = new ArrayList<SinkRecord>();
    	
        for (int i = 0; i < 10; i++) {
            sinkRecords.add( new SinkRecord(topic, 0, null, tableName, null, randomDataRecord(), i)    );
        }
        task.put(sinkRecords);
        
        task.flush(null);
        
        // Get the destination table name generated with config params
        String destinationTable = task.schemaMgr.getDestTable(topic, tableName);        
        // Get the Kinetica type from schema manager mapper collection from exact name and version
        KineticaFieldMapper mapper = task.schemaMgr.getFieldMapper(destinationTable, null);
        
        
        // Assume all fields got mapped, none missing
        assertEquals(mapper.getMapped().size(), 5);
        assertEquals(mapper.getMissing().size(), 0);
        
    	// Assume all number types got converted properly
        Column column = mapper.getMapped().get("test_boolean");
    	assertEquals(column.getColumnType(), com.gpudb.Type.Column.ColumnType.INTEGER);

    	column = mapper.getMapped().get("test_int");
    	assertEquals(column.getColumnType(), com.gpudb.Type.Column.ColumnType.INTEGER);
    	
        column = mapper.getMapped().get("test_long");
        assertEquals(column.getColumnType(), com.gpudb.Type.Column.ColumnType.LONG);
        
        column = mapper.getMapped().get("test_float");
        assertEquals(column.getColumnType(), com.gpudb.Type.Column.ColumnType.FLOAT);
       
        column = mapper.getMapped().get("test_double");
        assertEquals(column.getColumnType(), com.gpudb.Type.Column.ColumnType.DOUBLE);
       
        task.stop();      
    }

    @Test
    public void booleanToIntConversionKafkaInput() throws Exception {
        KineticaSinkTask task = new KineticaSinkTask();
        assertNotNull(task);
        task.start(this.config);        

        String topic = "kafka_topic_with_boolean";
        String tableName = "kafka_table_with_boolean";
        
        // Create random fake sinkRecords
    	List<SinkRecord> sinkRecords = new ArrayList<SinkRecord>();
    	
    	Schema KAFKA_SCHEMA = SchemaBuilder.struct()
                .name(tableName)
                .version(1)
                .field("test_boolean", Schema.BOOLEAN_SCHEMA)
                .field("test_int", Schema.INT32_SCHEMA)
                .field("test_long", Schema.INT64_SCHEMA)
                .field("test_float", Schema.FLOAT32_SCHEMA)
                .field("test_double", Schema.FLOAT64_SCHEMA)
                .build();
    	
        for (int i = 0; i < 10; i++) {
            sinkRecords.add( new SinkRecord(topic, 0, null, tableName, KAFKA_SCHEMA, randomDataRecord(), i)    );
        }
        
        task.put(sinkRecords);
        
        task.flush(null);
        
        // Get the destination table name generated with config params
        String destinationTable = task.schemaMgr.getDestTable(topic, tableName);        
        // Get the Kinetica type from schema manager mapper collection from exact name and version
        KineticaFieldMapper mapper = task.schemaMgr.getFieldMapper(destinationTable, 1);
        
        
        // Assume all fields got mapped, none missing
        assertEquals(mapper.getMapped().size(), 5);
        assertEquals(mapper.getMissing().size(), 0);
        
    	// Assume all number types got converted properly
        Column column = mapper.getMapped().get("test_boolean");
    	assertEquals(column.getColumnType(), com.gpudb.Type.Column.ColumnType.INTEGER);

    	column = mapper.getMapped().get("test_int");
    	assertEquals(column.getColumnType(), com.gpudb.Type.Column.ColumnType.INTEGER);
    	
        column = mapper.getMapped().get("test_long");
        assertEquals(column.getColumnType(), com.gpudb.Type.Column.ColumnType.LONG);
        
        column = mapper.getMapped().get("test_float");
        assertEquals(column.getColumnType(), com.gpudb.Type.Column.ColumnType.FLOAT);
       
        column = mapper.getMapped().get("test_double");
        assertEquals(column.getColumnType(), com.gpudb.Type.Column.ColumnType.DOUBLE);
       
        task.stop();      
    }
       

    /**
     * Helper function
     * Generates a given number of SinkRecords of key-value HashMap
     * @param number  number of records to generate
     * @return Collection of SinkRecords
     */
    private Collection<SinkRecord> generateJSON(int number) {
        List<SinkRecord> sinkRecords = new ArrayList<SinkRecord>();
        HashMap<String, Object> data = null;
        
        for (int i = 0; i < number; i++) {
            data = TweetRecord.generateHashMap();
            sinkRecords.add( new SinkRecord(TOPIC, PARTITION, null, TABLE, null, data, i)    );
        }
            
        return sinkRecords;
    }

    /**
     * Helper function for generating Kafka schema records
     */
    private HashMap<String, Object> randomDataRecord() {
        HashMap<String, Object> data = new HashMap<String, Object>();
        
        data.put("test_boolean", rnd.nextBoolean());
        data.put("test_int", rnd.nextInt(10));
        data.put("test_long", rnd.nextLong());
        data.put("test_float", rnd.nextFloat());
        data.put("test_double", rnd.nextDouble());

        return data;
    }
}
