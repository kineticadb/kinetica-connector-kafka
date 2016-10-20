package com.gpudb.kafka;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Range;
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
    public static ConfigDef CONFIG_DEF = new ConfigDef()
                .define(URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "GPUdb URL, e.g. 'http://localhost:9191'","GPUdb Properties",1,ConfigDef.Width.LONG,"GPUdb URL")
                .define(TIMEOUT_CONFIG, ConfigDef.Type.INT,DEFAULT_TIMEOUT,Range.atLeast(0), ConfigDef.Importance.HIGH, "GPUdb timeout (ms) (optional, default " + DEFAULT_TIMEOUT + "); 0 = no timeout","GPUdb Properties",2,ConfigDef.Width.SHORT,"Timeout")
                .define(TABLE_NAMES_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "GPUdb table names (comma-separated)","GPUdb Properties",3,ConfigDef.Width.LONG,"Table Names")
                .define(TOPIC_PREFIX_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Kafka topic prefix","GPUdb Properties",4,ConfigDef.Width.SHORT,"Topic Prefix")
                .define(USERNAME_CONFIG, ConfigDef.Type.STRING, "",ConfigDef.Importance.HIGH, "GPUdb username (optional)","GPUdb Properties",5,ConfigDef.Width.SHORT,"Username")
                .define(PASSWORD_CONFIG, ConfigDef.Type.STRING, "",ConfigDef.Importance.HIGH, "GPUdb password (optional)","GPUdb Properties",6,ConfigDef.Width.SHORT,"Password");

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    public void start(Map<String, String> props) {
        Map<String,Object> configParsed = GPUdbSourceConnector.CONFIG_DEF.parse(props); 
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
        return(CONFIG_DEF);
    }
}