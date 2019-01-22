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
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

}
