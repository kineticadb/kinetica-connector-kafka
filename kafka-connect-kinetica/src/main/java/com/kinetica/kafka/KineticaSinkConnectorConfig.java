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

public class KineticaSinkConnectorConfig extends AbstractConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(KineticaSinkConnectorConfig.class);

    // config params
    public static final String PARAM_URL               = "kinetica.url";
    public static final String PARAM_USERNAME          = "kinetica.username";
    public static final String PARAM_PASSWORD          = "kinetica.password";
    public static final String PARAM_TIMEOUT           = "kinetica.timeout";
    public static final String PARAM_ENABLE_MULTI_HEAD = "kinetica.enable_multihead";
    public static final String PARAM_RETRY_COUNT       = "kinetica.retry_count";
    public static final String PARAM_BATCH_SIZE        = "kinetica.batch_size";
    
    public static final String DEPRECATED_PARAM_COLLECTION                   = "kinetica.collection_name";
    public static final String DEPRECATED_PARAM_DEST_TABLE_OVERRIDE          = "kinetica.dest_table_override";
    public static final String DEPRECATED_PARAM_TABLE_PREFIX                 = "kinetica.table_prefix";
    public static final String DEPRECATED_PARAM_CREATE_TABLE                 = "kinetica.create_table";
    public static final String DEPRECATED_PARAM_ADD_NEW_FIELDS               = "kinetica.add_new_fields_as_columns"; 
    public static final String DEPRECATED_PARAM_MAKE_MISSING_FIELDS_NULLABLE = "kinetica.make_missing_field_nullable";
    public static final String DEPRECATED_PARAM_SINGLE_TABLE_PER_TOPIC       = "kinetica.single_table_per_topic";
    public static final String DEPRECATED_PARAM_ALLOW_SCHEMA_EVOLUTION       = "kinetica.allow_schema_evolution";
    public static final String DEPRECATED_PARAM_UPDATE_ON_EXISTING_PK        = "kinetica.update_on_existing_pk";

    public static final String PARAM_CREATE_TABLE           = "kinetica.tables.create_table";
    public static final String PARAM_TABLE_PREFIX           = "kinetica.tables.prefix";
    public static final String PARAM_SCHEMA                 = "kinetica.tables.schema_name";
    public static final String PARAM_DEST_TABLE_OVERRIDE    = "kinetica.tables.destination_name";
    public static final String PARAM_SINGLE_TABLE_PER_TOPIC = "kinetica.tables.single_table_per_topic";
    public static final String PARAM_UPDATE_ON_EXISTING_PK  = "kinetica.tables.update_on_existing_pk";

    public static final String PARAM_ALLOW_SCHEMA_EVOLUTION       = "kinetica.schema_evolution.enabled";
    public static final String PARAM_ADD_NEW_FIELDS               = "kinetica.schema_evolution.add_new_fields_as_columns";
    public static final String PARAM_MAKE_MISSING_FIELDS_NULLABLE = "kinetica.schema_evolution.make_missing_field_nullable";

    private static final String DEFAULT_TIMEOUT = "0";
    private static final String DEFAULT_BATCH_SIZE = "10000";
    public static final String DEFAULT_DOT_REPLACEMENT = "_";

    private static final String PARAM_GROUP = "Kinetica Properties";

    static ConfigDef config = baseConfigDef();
    private final String connectorName;

    public KineticaSinkConnectorConfig(Map<String, String> props) {
        this(config, props);
    }

    protected KineticaSinkConnectorConfig(ConfigDef config, Map<String, String> props) {
        super(config, props);
        connectorName = props.containsKey("name") ? props.get("name") : UUID.randomUUID().toString();
        // Validate destination table override value
        // When SinkConnector has a single_table_per_topic flag set, check the lengths of topics name list
        // and destination table override names list, exit with error if list lengths differ
        if ( new Boolean(props.get(PARAM_SINGLE_TABLE_PER_TOPIC)) &&
            !validateOverride(props.get(SinkTask.TOPICS_CONFIG), props.get(PARAM_DEST_TABLE_OVERRIDE))) {
            throw new ConnectException("Invalid configuration, with " +
                    PARAM_SINGLE_TABLE_PER_TOPIC + " = " + props.get(PARAM_SINGLE_TABLE_PER_TOPIC) + "\n" +
                    "expected exactly one destination table name per each topic name:\n" +
                    PARAM_DEST_TABLE_OVERRIDE + " = " + props.get(PARAM_DEST_TABLE_OVERRIDE) + "\n" +
                    SinkTask.TOPICS_CONFIG + " = " + props.get(SinkTask.TOPICS_CONFIG) + "\n" +
                    "Both parameters can be comma-separated lists of equal length or " + PARAM_DEST_TABLE_OVERRIDE + " can be left blank.");
        }
        if (!props.containsKey(PARAM_TABLE_PREFIX)) {
        	if (props.containsKey(DEPRECATED_PARAM_TABLE_PREFIX)) {
        		props.put(PARAM_TABLE_PREFIX, props.get(DEPRECATED_PARAM_TABLE_PREFIX));
        	} else {
        		props.put(PARAM_TABLE_PREFIX, config.configKeys().get(PARAM_TABLE_PREFIX).defaultValue.toString());
        	}
        }
        if (!props.containsKey(PARAM_DEST_TABLE_OVERRIDE)) {
        	if (props.containsKey(DEPRECATED_PARAM_DEST_TABLE_OVERRIDE)) {
        		props.put(PARAM_DEST_TABLE_OVERRIDE, props.get(DEPRECATED_PARAM_DEST_TABLE_OVERRIDE));
        	} else {
        		props.put(PARAM_DEST_TABLE_OVERRIDE, config.configKeys().get(PARAM_DEST_TABLE_OVERRIDE).defaultValue.toString());
        	}
        }
        if (!props.containsKey(PARAM_SCHEMA)) {
        	if (props.containsKey(DEPRECATED_PARAM_COLLECTION)) {
        		props.put(PARAM_SCHEMA, props.get(DEPRECATED_PARAM_COLLECTION));
        	} else {
        		props.put(PARAM_SCHEMA, config.configKeys().get(PARAM_SCHEMA).defaultValue.toString());
        	}
        }
        if (!props.containsKey(PARAM_SINGLE_TABLE_PER_TOPIC)) {
        	if (props.containsKey(DEPRECATED_PARAM_SINGLE_TABLE_PER_TOPIC)) {
        		props.put(PARAM_SINGLE_TABLE_PER_TOPIC, props.get(DEPRECATED_PARAM_SINGLE_TABLE_PER_TOPIC));
        	} else {
        		props.put(PARAM_SINGLE_TABLE_PER_TOPIC, config.configKeys().get(PARAM_SINGLE_TABLE_PER_TOPIC).defaultValue.toString());
        	}
        }
        if (!props.containsKey(PARAM_CREATE_TABLE)) {
        	if (props.containsKey(DEPRECATED_PARAM_CREATE_TABLE)) {
        		props.put(PARAM_CREATE_TABLE, props.get(DEPRECATED_PARAM_CREATE_TABLE));
        	} else {
        		props.put(PARAM_CREATE_TABLE, config.configKeys().get(PARAM_CREATE_TABLE).defaultValue.toString());
        	}
        }
        if (!props.containsKey(PARAM_ADD_NEW_FIELDS)) {
        	if (props.containsKey(DEPRECATED_PARAM_ADD_NEW_FIELDS)) {
        		props.put(PARAM_ADD_NEW_FIELDS, props.get(DEPRECATED_PARAM_ADD_NEW_FIELDS));
        	} else {
        		props.put(PARAM_ADD_NEW_FIELDS, config.configKeys().get(PARAM_ADD_NEW_FIELDS).defaultValue.toString());
        	}
        }
        if (!props.containsKey(PARAM_MAKE_MISSING_FIELDS_NULLABLE)) {
        	if (props.containsKey(DEPRECATED_PARAM_MAKE_MISSING_FIELDS_NULLABLE)) {
        		props.put(PARAM_MAKE_MISSING_FIELDS_NULLABLE, props.get(DEPRECATED_PARAM_MAKE_MISSING_FIELDS_NULLABLE));
        	} else {
        		props.put(PARAM_MAKE_MISSING_FIELDS_NULLABLE, config.configKeys().get(PARAM_MAKE_MISSING_FIELDS_NULLABLE).defaultValue.toString());
        	}
        }
        if (!props.containsKey(PARAM_ALLOW_SCHEMA_EVOLUTION)) {
        	if (props.containsKey(DEPRECATED_PARAM_ALLOW_SCHEMA_EVOLUTION)) {
        		props.put(PARAM_ALLOW_SCHEMA_EVOLUTION, props.get(DEPRECATED_PARAM_ALLOW_SCHEMA_EVOLUTION));
        	} else {
        		props.put(PARAM_ALLOW_SCHEMA_EVOLUTION, config.configKeys().get(PARAM_ALLOW_SCHEMA_EVOLUTION).defaultValue.toString());
        	}
        }
        if (!props.containsKey(PARAM_UPDATE_ON_EXISTING_PK)) {
        	if (props.containsKey(DEPRECATED_PARAM_UPDATE_ON_EXISTING_PK)) {
        		props.put(PARAM_UPDATE_ON_EXISTING_PK, props.get(DEPRECATED_PARAM_UPDATE_ON_EXISTING_PK));
        	} else {
        		props.put(PARAM_UPDATE_ON_EXISTING_PK, config.configKeys().get(PARAM_UPDATE_ON_EXISTING_PK).defaultValue.toString());
        	}
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
     * Returns basic Sink Connector configuration
     * @return ConfigDef
     */
    public static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(PARAM_URL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Kinetica URL, e.g. 'http://localhost:9191'", PARAM_GROUP, 1, ConfigDef.Width.SHORT,
                        "Kinetica URL")

                .define(PARAM_USERNAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                        "Kinetica username (optional)", PARAM_GROUP, 2, ConfigDef.Width.SHORT, "Username")

                .define(PARAM_PASSWORD, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                        "Kinetica password (optional)", PARAM_GROUP, 3, ConfigDef.Width.SHORT, "Password")

                .define(PARAM_SCHEMA, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                        "Kinetica schema name (optional, default empty)", PARAM_GROUP, 4,
                        ConfigDef.Width.LONG, "Schema Name")

                .define(PARAM_TABLE_PREFIX, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                        "Prefix applied to tablenames from Kafka schema. (optional)", PARAM_GROUP, 5,
                        ConfigDef.Width.LONG, "Table Prefix")

                .define(PARAM_DEST_TABLE_OVERRIDE, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                        "Table name that will replace name automatically generated from the schema. (optional)",
                        PARAM_GROUP, 6, ConfigDef.Width.LONG, "Table Override")

                .define(PARAM_TIMEOUT, ConfigDef.Type.INT, DEFAULT_TIMEOUT, Range.atLeast(0), ConfigDef.Importance.LOW,
                        "Kinetica timeout (ms) (optional, default " + DEFAULT_TIMEOUT + "); 0 = no timeout",
                        PARAM_GROUP, 7, ConfigDef.Width.SHORT, "Connection Timeout")

                .define(PARAM_BATCH_SIZE, ConfigDef.Type.INT, DEFAULT_BATCH_SIZE, Range.atLeast(1),
                        ConfigDef.Importance.LOW, "Kinetica batch size (optional, default " + DEFAULT_BATCH_SIZE + ")",
                        PARAM_GROUP, 8, ConfigDef.Width.SHORT, "Batch Size")

                .define(PARAM_CREATE_TABLE, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW,
                        "Create missing tables. (optional, default true)", PARAM_GROUP, 9, ConfigDef.Width.SHORT,
                        "Create Table")

                .define(PARAM_SINGLE_TABLE_PER_TOPIC, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW,
                        "Creates a single kinetica table per each Kafka topic. (optional, default false)",
                        PARAM_GROUP, 10, ConfigDef.Width.SHORT, "Single table per topic")

                .define(PARAM_ADD_NEW_FIELDS, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW,
                        "Add new field names as columns to Kinetica table. (optional, default false)",
                        PARAM_GROUP, 11, ConfigDef.Width.SHORT,
                        "Add new columns")

                .define(PARAM_MAKE_MISSING_FIELDS_NULLABLE, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW,
                        "Make missing from schema fields nullable columns in Kinetica table. (optional, default false)",
                        PARAM_GROUP, 12, ConfigDef.Width.SHORT,
                        "Alter existing column to nullable")

                .define(PARAM_RETRY_COUNT, ConfigDef.Type.INT, 1, Range.atLeast(1),
                        ConfigDef.Importance.LOW, "Number of attempts to insert record into Kinetica table. (optional, default 1)",
                        PARAM_GROUP, 13, ConfigDef.Width.SHORT, "Retry count")

                .define(PARAM_ALLOW_SCHEMA_EVOLUTION, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW,
                        "Schema evolution enabled for incoming Kafka messages. (optional, default false)", PARAM_GROUP, 14, ConfigDef.Width.SHORT,
                        "Schema evolution enabled")

                .define(PARAM_UPDATE_ON_EXISTING_PK, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW,
                        "Force update on existing PK when inserting Kafka messages. (optional, default true)", 
                        PARAM_GROUP, 15, ConfigDef.Width.SHORT, "Force update on existing PK")
        
                .define(PARAM_ENABLE_MULTI_HEAD, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW,
                        "Allow multi-head data ingest. (optional, default true)", PARAM_GROUP, 16, ConfigDef.Width.SHORT,
                        "Allow multi-head data ingest")
                
                .define(DEPRECATED_PARAM_COLLECTION, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW,
                        "Kinetica collection name (deprecated, use " + PARAM_SCHEMA + " instead)", PARAM_GROUP, 17,
                        ConfigDef.Width.LONG, String.format("%s (deprecated)", DEPRECATED_PARAM_COLLECTION))

                .define(DEPRECATED_PARAM_TABLE_PREFIX, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW,
                        "Prefix applied to tablenames from Kafka schema. (deprecated, use " + PARAM_TABLE_PREFIX + " instead)", 
                        PARAM_GROUP, 18, ConfigDef.Width.LONG, String.format("%s (deprecated)", DEPRECATED_PARAM_TABLE_PREFIX))

                .define(DEPRECATED_PARAM_DEST_TABLE_OVERRIDE, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW,
                        "Table name that will replace name automatically generated from the schema. (deprecated, use " + 
                        PARAM_DEST_TABLE_OVERRIDE + " instead)",
                        PARAM_GROUP, 19, ConfigDef.Width.LONG, String.format("%s (deprecated)", DEPRECATED_PARAM_DEST_TABLE_OVERRIDE))

                .define(DEPRECATED_PARAM_CREATE_TABLE, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW,
                        "Create missing tables. (deprecated, use " + PARAM_CREATE_TABLE + " instead)", PARAM_GROUP, 20, ConfigDef.Width.SHORT,
                        String.format("%s (deprecated)", DEPRECATED_PARAM_CREATE_TABLE))

                .define(DEPRECATED_PARAM_SINGLE_TABLE_PER_TOPIC, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW,
                        "Creates a single kinetica table per each Kafka topic. (deprecated, use " + PARAM_SINGLE_TABLE_PER_TOPIC + " instead)",
                        PARAM_GROUP, 21, ConfigDef.Width.SHORT, String.format("%s (deprecated)", DEPRECATED_PARAM_SINGLE_TABLE_PER_TOPIC))

                .define(DEPRECATED_PARAM_ADD_NEW_FIELDS, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW,
                        "Add new field names as columns to Kinetica table. (deprecated, use " + PARAM_ADD_NEW_FIELDS + 
                        " instead)", PARAM_GROUP, 22, ConfigDef.Width.SHORT,
                        String.format("%s (deprecated)", DEPRECATED_PARAM_ADD_NEW_FIELDS))

                .define(DEPRECATED_PARAM_MAKE_MISSING_FIELDS_NULLABLE, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW,
                        "Make missing from schema fields nullable columns in Kinetica table. (deprecated, use " + 
                        PARAM_MAKE_MISSING_FIELDS_NULLABLE + " instead)", PARAM_GROUP, 23, ConfigDef.Width.SHORT,
                        String.format("%s (deprecated)", DEPRECATED_PARAM_MAKE_MISSING_FIELDS_NULLABLE))

                .define(DEPRECATED_PARAM_ALLOW_SCHEMA_EVOLUTION, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW,
                        "Allow schema evolution for incoming Kafka messages. (deprecated, use " + DEPRECATED_PARAM_ALLOW_SCHEMA_EVOLUTION 
                        + " instead)", PARAM_GROUP, 24, ConfigDef.Width.SHORT, String.format("%s (deprecated)", DEPRECATED_PARAM_ALLOW_SCHEMA_EVOLUTION))

                .define(DEPRECATED_PARAM_UPDATE_ON_EXISTING_PK, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW,
                        "Allow update on existing PK when inserting Kafka messages. (deprecated, use " + PARAM_UPDATE_ON_EXISTING_PK + 
                        " instead)", PARAM_GROUP, 25, ConfigDef.Width.SHORT,
                        String.format("%s (deprecated)", DEPRECATED_PARAM_UPDATE_ON_EXISTING_PK));

    }

    public static void main(String[] args) {
        System.out.println(config.toRst());
    }

    /**
     * Validates the table override parameters
     *
     * @param topicNames list of topics as a String
     * @param tableOverrideNames list of table names overriding topic names in Kinetica, could be left empty
     *
     * @return whether tableOverrideNames is well-formed and override is possible
     */
    private static boolean validateOverride (String topicNames, String tableOverrideNames) {
        if (tableOverrideNames == null || tableOverrideNames.isEmpty()) {
            // no override to be performed
            return true;
        }
        if (topicNames!=null && !topicNames.isEmpty()) {
            if (!topicNames.contains(",") && !tableOverrideNames.contains(",")) {
                // single topic name found and single override name found
                return true;
            }
            if (topicNames.contains(",") && tableOverrideNames.contains(",") &&
                    topicNames.split(",").length == tableOverrideNames.split(",").length) {
                // both topics and override names are comma-separated lists of equal size
                return true;
            }
        }
        // no one-to-one topic name override possible
        return false;
    }

}
