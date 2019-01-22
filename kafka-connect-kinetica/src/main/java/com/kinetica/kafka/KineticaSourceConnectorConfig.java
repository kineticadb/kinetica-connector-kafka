package com.kinetica.kafka;

import java.util.Map;
import java.util.UUID;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KineticaSourceConnectorConfig extends AbstractConfig {
    private static final Logger LOG = LoggerFactory.getLogger(KineticaSourceConnectorConfig.class);

    // Config params
    public static final String PARAM_URL = "kinetica.url";
    public static final String PARAM_USERNAME = "kinetica.username";
    public static final String PARAM_PASSWORD = "kinetica.password";
    public static final String PARAM_TIMEOUT = "kinetica.timeout";
    public static final String PARAM_TABLE_NAMES = "kinetica.table_names";
    public static final String PARAM_TOPIC_PREFIX = "kinetica.topic_prefix";
    public static final String PARAM_SCHEMA_VERSION = "kinetica.kafka_schema_version";

    private static final String PARAM_GROUP = "Kinetica Properties";
    private static final String DEFAULT_TIMEOUT = "0";

    static ConfigDef config = baseConfigDef();
    private final String connectorName;

    public KineticaSourceConnectorConfig(Map<String, String> props) {
        this(config, props);
    }

    protected KineticaSourceConnectorConfig(ConfigDef config, Map<String, String> props) {
        super(config, props);
        connectorName = props.containsKey("name") ? props.get("name") : UUID.randomUUID().toString();
        if (!validateVersions(props.get(PARAM_TABLE_NAMES), props.get(PARAM_SCHEMA_VERSION))) {
            throw new ConnectException("Invalid configuration, expected exactly one version value per each table name:\n" + 
            		PARAM_SCHEMA_VERSION + "=" + props.get(PARAM_SCHEMA_VERSION) + "\n" +
            		PARAM_TABLE_NAMES + "=" + props.get(PARAM_TABLE_NAMES) + "\n" + 
                    "Both parameters can be comma-separated lists of equal length or " + PARAM_SCHEMA_VERSION + " can be left blank.");
        }
    }

    /**
     * Returns unique Connector name (used to manage Connectors through REST interface)
     * @return Connector name
     */
    public String getConnectorName() {
        return connectorName;
    }
    
    /**
     * Returns basic Source Connector configuration
     * @return ConfigDef
     */
    public static ConfigDef baseConfigDef() {
    	return new ConfigDef()
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

                .define(PARAM_TOPIC_PREFIX, ConfigDef.Type.STRING, "",
                        ConfigDef.Importance.MEDIUM,
                        "Prefix to prepend to source table when generating topic name.",
                        PARAM_GROUP, 4, ConfigDef.Width.SHORT, "Topic Prefix")

                .define(PARAM_SCHEMA_VERSION, ConfigDef.Type.STRING, "",
                        ConfigDef.Importance.MEDIUM,
                        "Supported schema version of Kafka topic if it already exists.",
                        PARAM_GROUP, 5, ConfigDef.Width.SHORT, "Topic Version(s)")

                .define(PARAM_USERNAME, ConfigDef.Type.STRING, "",
                        ConfigDef.Importance.MEDIUM,
                        "Kinetica username (optional)",
                        PARAM_GROUP, 6, ConfigDef.Width.SHORT, "Username")

                .define(PARAM_PASSWORD, ConfigDef.Type.STRING, "",
                        ConfigDef.Importance.MEDIUM,
                        "Kinetica password (optional)",
                        PARAM_GROUP, 7, ConfigDef.Width.SHORT, "Password");
    }

    public static void main(String[] args) {
        System.out.println(config.toRst());
    }
    
    
    /**
     * Validates the table override parameters
     *
     * @param tableNames list of Kinetica tables as a String
     * @param topicVersions list of schema versions for corresponding topics, could be left empty
     * 
     * @return whether topicVersions list is well-formed and override is possible
     */    
    private static boolean validateVersions (String tableNames, String topicVersions) {
        if (topicVersions == null || topicVersions.isEmpty()) {
            // no version mapping to be performed, default schema version would be expected 
            return true;
        }
        if (tableNames!=null && !tableNames.isEmpty()) { 
            if (!tableNames.contains(",") && !topicVersions.contains(",")) {
                // single topic name found and single schema version found
                return true;
            }
            if (tableNames.contains(",") && topicVersions.contains(",") &&
            		tableNames.split(",").length == topicVersions.split(",").length) {
                // both topics and schema versions are comma-separated lists of equal size
                return true;
            }
        }
        // no one-to-one topic to schema version mapping possible   
        return false;
    }

}
