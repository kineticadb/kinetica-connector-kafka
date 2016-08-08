package com.gpudb.kafka;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

/**
 * Kafka SourceConnector for streaming data from a GPUdb table.
 * 
 * The SourceConnector is used to configure the {@link GPUdbSourceTask}, which
 * performs the work of pulling data from the source into Kafka.
 */
public class GPUdbSourceConnector extends SourceConnector {
    /** Config file key for GPUdb URL */
    public static final String URL_CONFIG = "gpudb.url";
    /** Config file key for GPUdb username */
    public static final String USERNAME_CONFIG = "gpudb.username";
    /** Config file key for GPUdb password */
    public static final String PASSWORD_CONFIG = "gpudb.password";
    /** Config file key for GPUdb request/response timeouts */
    public static final String TIMEOUT_CONFIG = "gpudb.timeout";
    /** Config file key for names of GPUdb tables to use as streaming sources */
    public static final String TABLE_NAMES_CONFIG = "gpudb.table_names";
    /** Config file key for token prepended to each source table name to form
     *  the name of the corresponding Kafka topic into which those records will
     *  be queued */
    public static final String TOPIC_PREFIX_CONFIG = "topic_prefix";

    private static final String DEFAULT_TIMEOUT = "0";

    private Map<String, String> config;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    @SuppressWarnings("ResultOfObjectAllocationIgnored")
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

        if (!props.containsKey(TABLE_NAMES_CONFIG))
        {
            throw new IllegalArgumentException("Missing table names.");
        }

        config.put(TABLE_NAMES_CONFIG, props.get(TABLE_NAMES_CONFIG));

        if (!props.containsKey(TOPIC_PREFIX_CONFIG))
        {
            throw new IllegalArgumentException("Missing topic.");
        }

        config.put(TOPIC_PREFIX_CONFIG, props.get(TOPIC_PREFIX_CONFIG));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return GPUdbSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<String> tables = Arrays.asList(config.get(TABLE_NAMES_CONFIG).split(","));

        int partitionSize = tables.size() / maxTasks;
        int partitionExtras = tables.size() % maxTasks;
        List<Map<String, String>> taskConfigs = new ArrayList<>();

        for (int i = 0; i < maxTasks; i++) {
            int partitionStart = i * partitionSize + Math.min(i, partitionExtras);
            int partitionEnd = (i + 1) * partitionSize + Math.min(i + 1, partitionExtras);

            if (partitionStart == partitionEnd) {
                break;
            }

            Map<String, String> taskConfig = new HashMap<>(config);
            taskConfig.put(TABLE_NAMES_CONFIG, String.join(",", tables.subList(partitionStart, partitionEnd)));
            taskConfigs.add(taskConfig);
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
                .define(TABLE_NAMES_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "GPUdb table names (comma-separated)")
                .define(TOPIC_PREFIX_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Kafka topic prefix");
    }
}