package com.gpudb.kafka;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
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

    private static final String DEFAULT_TIMEOUT = "0";
    private static final String DEFAULT_BATCH_SIZE = "10000";

    private Map<String, String> config;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        config = new HashMap<>();

        if (!props.containsKey(URL_CONFIG))
        {
            throw new IllegalArgumentException("Missing URL.");
        }

        try {
            new URL(props.get(URL_CONFIG));
        } catch (MalformedURLException ex) {
            throw new IllegalArgumentException("Invalid URL (" + props.get(URL_CONFIG) + ").");
        }

        config.put(URL_CONFIG, props.get(URL_CONFIG));

        if (props.containsKey(USERNAME_CONFIG)) {
            config.put(USERNAME_CONFIG, props.get(USERNAME_CONFIG));
        } else {
            config.put(USERNAME_CONFIG, "");
        }

        if (props.containsKey(PASSWORD_CONFIG)) {
            config.put(PASSWORD_CONFIG, props.get(PASSWORD_CONFIG));
        } else {
            config.put(PASSWORD_CONFIG, "");
        }

        if (props.containsKey(TIMEOUT_CONFIG)) {
            try {
                if (Integer.parseInt(props.get(TIMEOUT_CONFIG)) < 0) {
                    throw new Exception();
                }
            } catch (Exception ex) {
                throw new IllegalArgumentException("Invalid timeout (" + props.get(TIMEOUT_CONFIG) + ").");
            }

            config.put(TIMEOUT_CONFIG, props.get(TIMEOUT_CONFIG));
        } else {
            config.put(TIMEOUT_CONFIG, DEFAULT_TIMEOUT);
        }

        if (props.containsKey(COLLECTION_NAME_CONFIG)) {
            config.put(COLLECTION_NAME_CONFIG, props.get(COLLECTION_NAME_CONFIG));
        } else {
            config.put(COLLECTION_NAME_CONFIG, "");
        }

        if (!props.containsKey(TABLE_NAME_CONFIG)) {
            throw new IllegalArgumentException("Missing table name.");
        }

        config.put(TABLE_NAME_CONFIG, props.get(TABLE_NAME_CONFIG));

        if (props.containsKey(BATCH_SIZE_CONFIG)) {
            try {
                if (Integer.parseInt(props.get(BATCH_SIZE_CONFIG)) < 1) {
                    throw new Exception();
                }
            } catch (Exception ex) {
                throw new IllegalArgumentException("Invalid batch size (" + props.get(BATCH_SIZE_CONFIG) + ").");
            }

            config.put(BATCH_SIZE_CONFIG, props.get(BATCH_SIZE_CONFIG));
        } else {
            config.put(BATCH_SIZE_CONFIG, DEFAULT_BATCH_SIZE);
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
        return new ConfigDef()
                .define(URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "GPUdb URL, e.g. 'http://localhost:9191'")
                .define(USERNAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "GPUdb username (optional)")
                .define(PASSWORD_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "GPUdb password (optional)")
                .define(TIMEOUT_CONFIG, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "GPUdb timeout (ms) (optional, default " + DEFAULT_TIMEOUT + "); 0 = no timeout")
                .define(COLLECTION_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "GPUdb collection name (optional, default--no collection name)")
                .define(TABLE_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "GPUdb table name")
                .define(BATCH_SIZE_CONFIG, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "GPUdb batch size (optional, default " + DEFAULT_BATCH_SIZE + ")");
    }
}