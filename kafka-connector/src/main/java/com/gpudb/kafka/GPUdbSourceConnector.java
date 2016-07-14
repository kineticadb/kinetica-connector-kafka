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
import org.apache.kafka.connect.source.SourceConnector;

public class GPUdbSourceConnector extends SourceConnector {
    public static final String URL_CONFIG = "gpudb.url";
    public static final String USERNAME_CONFIG = "gpudb.username";
    public static final String PASSWORD_CONFIG = "gpudb.password";
    public static final String TIMEOUT_CONFIG = "gpudb.timeout";
    public static final String TABLE_NAME_CONFIG = "gpudb.table_name";
    public static final String TOPIC_CONFIG = "topic";

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
            config.put(TIMEOUT_CONFIG, "0");
        }

        if (!props.containsKey(TABLE_NAME_CONFIG))
        {
            throw new IllegalArgumentException("Missing table name.");
        }

        config.put(TABLE_NAME_CONFIG, props.get(TABLE_NAME_CONFIG));

        if (!props.containsKey(TOPIC_CONFIG))
        {
            throw new IllegalArgumentException("Missing topic.");
        }

        config.put(TOPIC_CONFIG, props.get(TOPIC_CONFIG));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return GPUdbSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        taskConfigs.add(config);
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
                .define(TIMEOUT_CONFIG, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "GPUdb timeout (ms); 0 = no timeout")
                .define(TABLE_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "GPUdb table name")
                .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Kafka topic");
    }
}