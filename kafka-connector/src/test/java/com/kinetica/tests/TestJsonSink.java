package com.kinetica.tests;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kinetica.kafka.KineticaSinkTask;

public class TestJsonSink {

    private final static Logger LOG = LoggerFactory.getLogger(TestJsonSink.class);

    private  Map<String,String> sinkConfig = null;

    @Before
    public void setup() throws Exception {
        this.sinkConfig = TestConnector.getConfig("src/test/resources/sink.properties");
    }

    @Test
    public void testStart() throws Exception {

        ArrayList<SinkRecord> sinkRecords = new ArrayList<>();
        sinkRecords.add(jsonToSink("src/test/resources/TX_TP_FIN_EVENT.json"));
        sinkRecords.add(jsonToSink("src/test/resources/RX_TX_FIN_EVENT.json"));
        sinkRecords.add(jsonToSink("src/test/resources/TX_TP_FIN_EVENT.json"));
        sinkRecords.add(jsonToSink("src/test/resources/RX_TX_FIN_EVENT.json"));

        KineticaSinkTask sinkTask = TestConnector.startSinkConnector(this.sinkConfig);
        sinkTask.put(sinkRecords);
        sinkTask.flush(null);
        sinkTask.stop();

        LOG.info("Test Complete!");
    }

    private SinkRecord jsonToSink(String fileName) throws IOException {
        File oggJsonFile = new File(fileName);
        String oggJson = FileUtils.readFileToString(oggJsonFile, StandardCharsets.UTF_8.name());
        HashMap<String, Object> value = convertToConnect(oggJson);
        SinkRecord record = makeSinkRecord(value);
        return record;
    }

    private SinkRecord makeSinkRecord(HashMap<String, Object> value) {
        String topic = "topic";
        Schema keySchema = null;
        Object key = value.get("table");
        Schema valueSchema = null;

        SinkRecord sincRec = new SinkRecord(topic, 0, keySchema, key, valueSchema, value, 0);
        return sincRec;
    }

    public HashMap<String, Object> convertToConnect(String json) {
        JsonConverter converter = new JsonConverter();

        HashMap<String, Object> config = new HashMap<>();
        config.put("schemas.enable", false);
        converter.configure(config, false);

        SchemaAndValue cData = converter.toConnectData("topic", json.getBytes());

        @SuppressWarnings("unchecked")
        HashMap<String, Object> value = (HashMap<String, Object>)cData.value();

        return value;
    }

}
