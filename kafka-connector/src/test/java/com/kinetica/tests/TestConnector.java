package com.kinetica.tests;

import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpudb.BulkInserter;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase;
import com.gpudb.RecordObject;
import com.gpudb.Type;
import com.gpudb.protocol.CreateTableRequest;
import com.kinetica.kafka.KineticaSinkConnector;
import com.kinetica.kafka.KineticaSinkTask;
import com.kinetica.kafka.KineticaSourceConnector;
import com.kinetica.kafka.KineticaSourceTask;
import com.kinetica.kafka.tests.TweetRecord;

/**
 * The testConnector JUnit is a self-contained unit test of source and sink connectors
 * that should run in eclipse with minimal configuration. Steps are as follows:
 * <ol>
 * <li>This class makes use of <b>source.properties</b> and <b>sink.properties</b>. You will need
 *      to edit at least the URL of gpudb in these files.
 * <li>Run {@link #createSourceTable()} to create the table configured in <b>kinetica.table_names</b>.
 * <li>Run {@link #testConnector()} to copy data from the source to sink tables.
 * </ol>
 * <p>
 * Note: To run a JUnit from Eclipse select the test method and choose
 *       [right click]->[Run As]->[Junit Test].
 * @author Chad Juliano
 */
public class TestConnector {

    private final static Logger LOG = LoggerFactory.getLogger(TestConnector.class);
    private static final long NUM_RECS = 10;

    private  Map<String,String> sourceConfig = null;
    private  Map<String,String> sinkConfig = null;

    @Before
    public void setup() throws Exception {
        this.sourceConfig = getConfig("src/test/resources/source.properties");
        this.sinkConfig = getConfig("src/test/resources/sink.properties");
    }

    /**
     * This will invoke the source and sink connectors to copy data from the table
     * configured in <b>source.properties</b> (KafkaConnectorTest) to the one configured in
     * <b>sink.properties</b> (TwitterDest).
     * <p>
     * Note: Make sure that you have run {@link #createSourceTable()} to create KafkaConnectorTest.
     * @throws Exception
     */
    @Test
    public void testConnector() throws Exception {

        try {
            KineticaSourceTask sourceTask = startSourceConnector(this.sourceConfig);

            // wait for ZMQ to start
            Thread.sleep(1000);

            insertTableRecs(this.sourceConfig, NUM_RECS);

            // wait for ZMQ to pick up the rows
            Thread.sleep(1000);

            // get the results
            List<SourceRecord> sourceRecords = sourceTask.poll();

            LOG.info("Stopping source task...");
            sourceTask.stop();

            ArrayList<SinkRecord> sinkRecords = convertSourceToSinc(sourceRecords);

            LOG.info("Sending records to sink...");
            KineticaSinkTask sinkTask = startSinkConnector(this.sinkConfig);
            sinkTask.put(sinkRecords);
            sinkTask.flush(null);
            sinkTask.stop();

            LOG.info("Test Complete!");
        }
        catch(Exception ex) {
            LOG.error("Test failed", ex);
            throw ex;
        }
    }

    /**
     * Drop/create the table configured in <b>source.properties</b> with the columns specified
     * in {@link com.kinetica.kafka.tests.TweetRecord}.
     * @throws Exception
     */
    @Test
    public void createSourceTable() throws Exception {
        final String COLLECTION_NAME = "TEST";

        try {
                Map<String,String> sourceConfig = getConfig("src/test/resources/source.properties");
                String gpudbURL = sourceConfig.get(KineticaSourceConnector.URL_CONFIG);
                String tableName = sourceConfig.get(KineticaSourceConnector.TABLE_NAMES_CONFIG);

            GPUdb gpudb = new GPUdb(gpudbURL);
            String typeId = RecordObject.createType(TweetRecord.class, gpudb);

            if(gpudb.hasTable(tableName, null).getTableExists()) {
                    LOG.info("Dropping table: {}", tableName);
                    gpudb.clearTable(tableName, null, null);
            }

                LOG.info("Creating table: {}", tableName);
                Map<String, String> options = GPUdbBase.options(
                        CreateTableRequest.Options.COLLECTION_NAME, COLLECTION_NAME);
            gpudb.createTable(tableName, typeId, options);

            LOG.info("Done!");
        }
        catch (Exception ex) {
            LOG.error("Test failed", ex);
            throw ex;
        }
    }

    public ArrayList<SinkRecord> convertSourceToSinc(List<SourceRecord> sourceList) {
        ArrayList<SinkRecord> sincRecords = new ArrayList<>(sourceList.size());

        LOG.info("Conveting source records: {}", sourceList.size());

        for(int recNum=0; recNum < sourceList.size(); recNum++) {
            SourceRecord sourceRec = sourceList.get(recNum);

            String topic = sourceRec.topic();
            //Integer partition = sourceRec.kafkaPartition();
            Schema keySchema = sourceRec.keySchema();
            Object key = sourceRec.key();
            Schema valueSchema = sourceRec.valueSchema();
            Object value = sourceRec.value();

            SinkRecord sincRec = new SinkRecord(topic, 0, keySchema, key, valueSchema, value, recNum);
            sincRecords.add(sincRec);
        }

        return sincRecords;
    }

    public KineticaSourceTask startSourceConnector(Map<String,String> sourceConfig) throws Exception {
        KineticaSourceConnector sourceConnector = new KineticaSourceConnector();
        sourceConnector.start(sourceConfig);

        List<Map<String, String>> taskConfigs = sourceConnector.taskConfigs(1);
        Map<String, String> taskConfig = taskConfigs.get(0);

        KineticaSourceTask task = new KineticaSourceTask();
        LOG.info("Starting source task...");
        task.start(taskConfig);

        return task;
    }

    public KineticaSinkTask startSinkConnector(Map<String,String> sinkConfig) throws Exception {
        KineticaSinkConnector sinkConnector = new KineticaSinkConnector();
        sinkConnector.start(sinkConfig);

        List<Map<String, String>> taskConfigs = sinkConnector.taskConfigs(1);
        Map<String, String> config = taskConfigs.get(0);

        KineticaSinkTask task = new KineticaSinkTask();
        LOG.info("Starting sink task...");
        task.start(config);

        return task;
    }

    public void insertTableRecs(Map<String,String> sourceConfig, long numRecs) throws Exception {

        String gpudbURL = sourceConfig.get(KineticaSourceConnector.URL_CONFIG);
        String tableName = sourceConfig.get(KineticaSourceConnector.TABLE_NAMES_CONFIG);
        GPUdb gpudb = new GPUdb(gpudbURL);

        LOG.info("Generating {} records ", numRecs);
        ArrayList<TweetRecord> records = new ArrayList<>();
        for(int recNum = 0; recNum < numRecs; recNum++){
            records.add(TweetRecord.generateRandomRecord());
        }

        LOG.info("Inserting records.");
        Type recordtype = RecordObject.getType(TweetRecord.class);
        BulkInserter<TweetRecord> bulkInserter = new BulkInserter<>(gpudb, tableName, recordtype, 100, null);
        bulkInserter.insert(records);
        bulkInserter.flush();

        if(bulkInserter.getCountInserted() != numRecs) {
                throw new Exception(String.format("Added %d records but only %d were inserted.",
                        records.size(), bulkInserter.getCountInserted()));
        }

        records.clear();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Map<String,String> getConfig(String fileName) throws Exception {
        Properties props = new Properties();
        try (Reader propsReader = new FileReader(fileName)) {
            LOG.info("Loading properties: {}", fileName);
            props.load(propsReader);
        }

        return new HashMap<String,String>((Map)props);
    }
}
