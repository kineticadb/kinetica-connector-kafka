package com.kinetica.kafka;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Kafka SourceConnector for streaming data from a GPUdb table.
 *
 * The SourceConnector is used to configure the {@link KineticaSourceTask}, which
 * performs the work of pulling data from the source into Kafka.
 */
public class KineticaSourceConnector extends SourceConnector {

    private static final Logger LOG = LoggerFactory.getLogger(KineticaSinkTask.class);

    // Config params
    public static final String PARAM_URL = "kinetica.url";
    public static final String PARAM_USERNAME = "kinetica.username";
    public static final String PARAM_PASSWORD = "kinetica.password";
    public static final String PARAM_TIMEOUT = "kinetica.timeout";
    public static final String PARAM_TABLE_NAMES = "kinetica.table_names";
    public static final String TOPIC_PREFIX_CONFIG = "kinetica.topic_prefix";


    private static final String PARAM_GROUP = "Kinetica Properties";
    private static final String DEFAULT_TIMEOUT = "0";

    private final static ConfigDef CONFIG_DEF = new ConfigDef()
                .define(PARAM_URL, ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Kinetica URL, e.g. 'http://localhost:9191'",
                        PARAM_GROUP, 1, ConfigDef.Width.LONG, "Kinetica URL")

                .define(PARAM_TIMEOUT, ConfigDef.Type.INT, DEFAULT_TIMEOUT, Range.atLeast(0),
                        ConfigDef.Importance.LOW,
                        "Kinetica timeout (ms) (optional, default " + DEFAULT_TIMEOUT + "); 0 = no timeout",
                        PARAM_GROUP, 2, ConfigDef.Width.SHORT, "Timeout")

                .define(PARAM_TABLE_NAMES, ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Kinetica table names (comma-separated)",
                        PARAM_GROUP, 3, ConfigDef.Width.LONG, "Table Names")

                .define(TOPIC_PREFIX_CONFIG, ConfigDef.Type.STRING,
                        ConfigDef.Importance.MEDIUM,
                        "Prefix to prepend to source table when generating topic name.",
                        PARAM_GROUP, 4, ConfigDef.Width.SHORT, "Topic Prefix")

                .define(PARAM_USERNAME, ConfigDef.Type.STRING, "",
                        ConfigDef.Importance.MEDIUM,
                        "Kinetica username (optional)",
                        PARAM_GROUP, 5, ConfigDef.Width.SHORT, "Username")

                .define(PARAM_PASSWORD, ConfigDef.Type.STRING, "",
                        ConfigDef.Importance.MEDIUM,
                        "Kinetica password (optional)",
                        PARAM_GROUP, 6, ConfigDef.Width.SHORT, "Password");

    private final Map<String, String> config = new HashMap<>();

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {

        Map<String,Object> configParsed = KineticaSourceConnector.CONFIG_DEF.parse(props);
        for (Map.Entry<String, Object> entry : configParsed.entrySet()) {
            this.config.put(entry.getKey(), entry.getValue().toString());
        }

        String connectUrl = props.get(KineticaSourceConnector.PARAM_URL);
        try {
            new URL(connectUrl);
        } catch (MalformedURLException ex) {
            throw new IllegalArgumentException("Invalid URL (" + connectUrl + ").");
        }
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return KineticaSourceConnector.CONFIG_DEF;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return KineticaSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<String> tables = Arrays.asList(this.config.get(PARAM_TABLE_NAMES).split(","));

        int partitionSize = tables.size() / maxTasks;
        int partitionExtras = tables.size() % maxTasks;
        List<Map<String, String>> taskConfigs = new ArrayList<>();

        for (int taskNum = 0; taskNum < maxTasks; taskNum++) {
            int partitionStart = taskNum * partitionSize + Math.min(taskNum, partitionExtras);
            int partitionEnd = (taskNum + 1) * partitionSize + Math.min(taskNum + 1, partitionExtras);
            if (partitionStart == partitionEnd) {
                LOG.info("Not creating task<{}> (partitionStart == partitionEnd)", taskNum);
                break;
            }

            String tableNames = String.join(",", tables.subList(partitionStart, partitionEnd));
            LOG.info("Configuring task <{}> for tables [{}]", taskNum, tableNames);

            Map<String, String> taskConfig = new HashMap<>(this.config);
            taskConfig.put(PARAM_TABLE_NAMES, tableNames);
            taskConfigs.add(taskConfig);
        }

        return taskConfigs;
    }
}