package com.kinetica.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase;
import com.gpudb.GenericRecord;
import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.gpudb.protocol.ClearTableRequest;
import com.gpudb.protocol.CreateTableRequest;
import com.gpudb.protocol.GetRecordsResponse;
import com.gpudb.protocol.ShowTableResponse;
import com.kinetica.kafka.data.utils.SchemaRegistryUtils;
import com.kinetica.kafka.utils.ConnectorConfigHelper;

public class UpsertTickerDataTest {
    
    private final static String TICKER = "TickerTopic";
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
    
    // Kafka connector is configured to insert all records because Kinetica table has no PK 
    @Test 
    public void testInsert() throws Exception {
        // Connector is configured to have an additional table prefix
        String prefix = "Insert";
        Map<String, String> config = 
        		ConnectorConfigHelper.getParameterizedConfig(TICKER, COLLECTION, prefix, "", true, false, true, false, false, false);
        String tableName = prefix+TICKER;
        gpudb.clearTable(tableName, "", 
        		GPUdbBase.options(ClearTableRequest.Options.NO_ERROR_IF_NOT_EXISTS, ClearTableRequest.Options.TRUE));

        // Configure and start connector and sinktask
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 
        
        // run data generation
        runTickerSinkTask(task, TICKER);
        runTickerSinkTask(task, TICKER);
        runTickerSinkTask(task, TICKER);
        Thread.sleep(1000);
        
        boolean tableExists = gpudb.hasTable(prefix+TICKER, null).getTableExists(); 
        assertTrue(tableExists);
        
        // expect table size to match number of Kafka messages generated/ingested
        ShowTableResponse response = gpudb.showTable(prefix+TICKER, tableSizeProps);        
        int size = response.getFullSizes().get(0).intValue();
        // Kinetica table has 3 batches of data stored because 3 batches of data were sent  
        // and PK is not set on data table 
        assertEquals(size, 3 * batch_size);
        
    }

    // Kafka connector is configured to skip new records that have the same PK as existing data 
    @Test 
    public void testUpsertOff() throws Exception {
        // Connector is configured to have an additional table prefix
        String prefix = "UpsertOff";
        Map<String, String> config = 
        		ConnectorConfigHelper.getParameterizedConfig(TICKER, COLLECTION, prefix, "", true, false, true, false, false, false);
        String tableName = prefix+TICKER;
        gpudb.clearTable(tableName, "", 
        		GPUdbBase.options(ClearTableRequest.Options.NO_ERROR_IF_NOT_EXISTS, ClearTableRequest.Options.TRUE));
        		
        // Create Kinetica table with first field made PK
        List<Column> temp = KineticaTypeConverter.convertTypeFromSchema(SchemaRegistryUtils.KAFKA_TICKER_SCHEMA).getColumns();
        List<Column> columns = new ArrayList<Column>();
        for (Column col : temp) {
        	List<String> options = new ArrayList<String>(col.getProperties());
        	if (col.getName().equalsIgnoreCase(SchemaRegistryUtils.TICKER_PK)) {
        		options.add(com.gpudb.ColumnProperty.PRIMARY_KEY);
        	}
        	columns.add(new Column(col.getName(), col.getType(), options));
        }
        
        Type gpudbType = new Type(columns);
        String typeId = gpudbType.create(gpudb);
        if (gpudb.hasTable(prefix+TICKER, null).getTableExists()) {
        	
        }
        gpudb.createTable(tableName, typeId, GPUdbBase.options(CreateTableRequest.Options.COLLECTION_NAME, COLLECTION) );

        Thread.sleep(1000);
        
        // Assert that table was created successfully
        boolean tableExists = gpudb.hasTable(prefix+TICKER, null).getTableExists(); 
        assertTrue(tableExists);

        // Configure and start connector and sinktask
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 

        // run data generation
        // Keep the first batch as a reference dataset to compare with table data
        HashMap<String, SinkRecord> controlSet = runTickerSinkTask(task, TICKER);
        Thread.sleep(100);
        runTickerSinkTask(task, TICKER);
        Thread.sleep(100);
        runTickerSinkTask(task, TICKER);
        Thread.sleep(1000);
        
        ShowTableResponse response = gpudb.showTable(prefix+TICKER, tableSizeProps);        
        int size = response.getFullSizes().get(0).intValue();
        // despite the fact that 3 batches of data were sent through connector, Kinetica table
        // has one batch size of data stored (the oldest data) 
        assertEquals(size, batch_size);
        
        // get all table data
        GetRecordsResponse<GenericRecord> resp = gpudb.getRecords(tableName, 0, batch_size*3, null);
        
        for (GenericRecord rec : resp.getData()) {
        	// ticker symbol/PK found in dataset
        	assertTrue(controlSet.containsKey(rec.get("symbol")) );
        	// int bidSize matches in table data and reference set
        	assertEquals(rec.get("bidSize"), ((Struct)controlSet.get(rec.get("symbol")).value()).get("bidSize"));
        	// float bidPrice matches in table data and reference set
        	assertEquals(rec.get("bidPrice"), ((Struct)controlSet.get(rec.get("symbol")).value()).get("bidPrice"));
        	// long timestamp lastUpdated matches in table data and reference set
        	assertEquals(rec.get("lastUpdated"), ((Struct)controlSet.get(rec.get("symbol")).value()).get("lastUpdated"));
        }
        
    }

    // Kafka connector is configured to overwrite records on same PK with new data 
    @Test 
    public void testUpsertOn() throws Exception {
        // Connector is configured to have an additional table prefix
        String prefix = "UpsertOn";
        Map<String, String> config = ConnectorConfigHelper.getParameterizedConfig(TICKER, COLLECTION, prefix, "", true, false, true, false, false, true);
        String tableName = prefix+TICKER;
        gpudb.clearTable(tableName, "", 
        		GPUdbBase.options(ClearTableRequest.Options.NO_ERROR_IF_NOT_EXISTS, ClearTableRequest.Options.TRUE));
        		
        // Create Kinetica table with first field made PK
        List<Column> temp = KineticaTypeConverter.convertTypeFromSchema(SchemaRegistryUtils.KAFKA_TICKER_SCHEMA).getColumns();
        List<Column> columns = new ArrayList<Column>();
        for (Column col : temp) {
        	List<String> options = new ArrayList<String>(col.getProperties());
        	if (col.getName().equalsIgnoreCase(SchemaRegistryUtils.TICKER_PK)) {
        		options.add(com.gpudb.ColumnProperty.PRIMARY_KEY);
        	}
        	columns.add(new Column(col.getName(), col.getType(), options));
        }
        
        Type gpudbType = new Type(columns);
        String typeId = gpudbType.create(gpudb);
        if (gpudb.hasTable(prefix+TICKER, null).getTableExists()) {
        	
        }
        gpudb.createTable(tableName, typeId, GPUdbBase.options(CreateTableRequest.Options.COLLECTION_NAME, COLLECTION) );

        Thread.sleep(1000);
        
        // Assert that table was created successfully
        boolean tableExists = gpudb.hasTable(prefix+TICKER, null).getTableExists(); 
        assertTrue(tableExists);

        // Configure and start connector and sinktask
        KineticaSinkConnector connector = ConnectorConfigHelper.startConnector(config);
        KineticaSinkTask task = startSinkTask(connector); 

        // run data generation
        runTickerSinkTask(task, TICKER);
        Thread.sleep(100);
        runTickerSinkTask(task, TICKER);
        Thread.sleep(100);
        // Keep the last batch as a reference dataset to compare with table data
        HashMap<String, SinkRecord> controlSet = runTickerSinkTask(task, TICKER);
        Thread.sleep(1000);
        
        ShowTableResponse response = gpudb.showTable(prefix+TICKER, tableSizeProps);        
        int size = response.getFullSizes().get(0).intValue();
        // despite the fact that 3 batches of data were sent through connector, Kinetica table
        // has one batch size of data stored (the most recent data) 
        assertEquals(size, batch_size);
        
        // get all table data
        GetRecordsResponse<GenericRecord> resp = gpudb.getRecords(tableName, 0, batch_size*3, null);
        
        for (GenericRecord rec : resp.getData()) {
        	// ticker symbol/PK found in dataset
        	assertTrue(controlSet.containsKey(rec.get("symbol")) );
        	// int bidSize matches in table data and reference set
        	assertEquals(rec.get("bidSize"), ((Struct)controlSet.get(rec.get("symbol")).value()).get("bidSize"));
        	// float bidPrice matches in table data and reference set
        	assertEquals(rec.get("bidPrice"), ((Struct)controlSet.get(rec.get("symbol")).value()).get("bidPrice"));
        	// long timestamp lastUpdated matches in table data and reference set
        	assertEquals(rec.get("lastUpdated"), ((Struct)controlSet.get(rec.get("symbol")).value()).get("lastUpdated"));
        }
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
        taskConfig.put(SinkTask.TOPICS_CONFIG, TICKER);

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
    private HashMap<String, SinkRecord> runTickerSinkTask(KineticaSinkTask task, String topic) throws Exception {
                
        List<SinkRecord> sinkRecords;
        
        sinkRecords = SchemaTestSerializerDataPump.mockTickerValues(topic, batch_size);
        task.put(sinkRecords);
        
        Thread.sleep(1000);
        task.flush(null); 
                
        task.stop();
        
        // return a hashmap of sink records for value comparison, using symbol PK as key
        HashMap<String, SinkRecord> result = new HashMap<String, SinkRecord>();
        for (SinkRecord rec : sinkRecords) {
        	if (rec.valueSchema().type() == org.apache.kafka.connect.data.Schema.Type.STRUCT) {
        		Struct val = (Struct)rec.value();
        		result.put(val.getString("symbol"), rec);
        	}
        }
        return result;
    }

  
}
