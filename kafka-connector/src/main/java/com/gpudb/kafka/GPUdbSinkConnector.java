package com.gpudb.kafka;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/**
 * Kafka SinkConnector for streaming data to a GPUdb table.
 * 
 * The SinkConnector is used to configure the {@link GPUdbSinkTask}, which
 * performs the work of writing data from Kafka into the target table.
 */
public class GPUdbSinkConnector extends SinkConnector {
    /** Config file key for GPUdb URL */
    public static final String URL_CONFIG = "gpudb.url";
    /** Config file key for GPUdb username */
    public static final String USERNAME_CONFIG = "gpudb.username";
    /** Config file key for GPUdb password */
    public static final String PASSWORD_CONFIG = "gpudb.password";
    /** Config file key for GPUdb request/response timeouts */
    public static final String TIMEOUT_CONFIG = "gpudb.timeout";
    /** Config file key for name of GPUdb collection containing target table */
    public static final String COLLECTION_NAME_CONFIG = "gpudb.collection_name";
    /** Config file key for name of GPUdb table to use as streaming target */
    public static final String TABLE_NAME_CONFIG = "gpudb.table_name";
    /** Config file key for # of records to collect before writing to GPUdb */
    public static final String BATCH_SIZE_CONFIG = "gpudb.batch_size";

    public static final String DEFAULT_TIMEOUT = "1000";
    public static final String DEFAULT_BATCH_SIZE = "10000";

    private Map<String, String> config;
    public static ConfigDef CONFIG_DEF =  new ConfigDef()
                .define(URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "GPUdb URL, e.g. 'http://localhost:9191'","GPUdb Properties",1,ConfigDef.Width.SHORT,"GPUdb URL")
                .define(TIMEOUT_CONFIG, ConfigDef.Type.INT, DEFAULT_TIMEOUT, Range.atLeast(0),ConfigDef.Importance.HIGH, "GPUdb timeout (ms) (optional, default " + DEFAULT_TIMEOUT + "); 0 = no timeout","GPUdb Properties",2,ConfigDef.Width.SHORT,"Timeout")
                .define(COLLECTION_NAME_CONFIG, ConfigDef.Type.STRING, "",ConfigDef.Importance.HIGH, "GPUdb collection name (optional, default--no collection name)","GPUdb Properties",3,ConfigDef.Width.LONG,"Collection Name")
                .define(TABLE_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "GPUdb table name","GPUdb Properties",4,ConfigDef.Width.LONG,"Table Name")
                .define(USERNAME_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "GPUdb username (optional)","GPUdb Properties",5,ConfigDef.Width.SHORT,"Username")
                .define(PASSWORD_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "GPUdb password (optional)","GPUdb Properties",6,ConfigDef.Width.SHORT,"Password")
                .define(BATCH_SIZE_CONFIG, ConfigDef.Type.INT, DEFAULT_BATCH_SIZE, Range.atLeast(1), ConfigDef.Importance.HIGH, "GPUdb batch size (optional, default " + DEFAULT_BATCH_SIZE + ")","GPUdb Properties",7,ConfigDef.Width.SHORT,"Batch Size");


    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        Map<String,Object> configParsed = GPUdbSinkConnector.CONFIG_DEF.parse(props); 
        config = new HashMap<String,String>();
        for (Map.Entry<String, Object> entry : configParsed.entrySet()) {
            config.put(entry.getKey(), entry.getValue().toString());
        }        

        try {
            new URL(props.get(URL_CONFIG));
        } catch (MalformedURLException ex) {
            throw new IllegalArgumentException("Invalid URL (" + props.get(URL_CONFIG) + ").");
        }

    }

    @Override
    public Class<? extends Task> taskClass() {
        return GPUdbSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();

        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(new HashMap<>(config));
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return(GPUdbSinkConnector.CONFIG_DEF);
    }
}