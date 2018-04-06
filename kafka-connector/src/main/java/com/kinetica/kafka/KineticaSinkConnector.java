package com.kinetica.kafka;

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
 * The SinkConnector is used to configure the {@link KineticaSinkTask}, which
 * performs the work of writing data from Kafka into the target table.
 */
public class KineticaSinkConnector extends SinkConnector {

    // config params
	public static final String PARAM_URL = "kinetica.url";
	public static final String PARAM_USERNAME = "kinetica.username";
	public static final String PARAM_PASSWORD = "kinetica.password";
	public static final String PARAM_TIMEOUT = "kinetica.timeout";
	public static final String PARAM_COLLECTION = "kinetica.collection_name";
    public static final String PARAM_BATCH_SIZE = "kinetica.batch_size";
    public static final String PARAM_DEST_TABLE_OVERRIDE = "kinetica.dest_table_override";
	public static final String PARAM_TABLE_PREFIX = "kinetica.table_prefix";
    public static final String PARAM_CREATE_TABLE = "kinetica.create_table";

	private static final String DEFAULT_TIMEOUT = "0";
	private static final String DEFAULT_BATCH_SIZE = "10000";
	private static final String PARAM_GROUP = "Kinetica Properties";

	public static final ConfigDef CONFIG_DEF =  new ConfigDef()
                .define(PARAM_URL, ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Kinetica URL, e.g. 'http://localhost:9191'",
                        PARAM_GROUP, 1, ConfigDef.Width.SHORT, "Kinetica URL")

                .define(PARAM_USERNAME, ConfigDef.Type.STRING, "",
                        ConfigDef.Importance.MEDIUM,
                        "Kinetica username (optional)",
                        PARAM_GROUP, 2, ConfigDef.Width.SHORT, "Username")

                .define(PARAM_PASSWORD, ConfigDef.Type.STRING, "",
                        ConfigDef.Importance.MEDIUM,
                        "Kinetica password (optional)",
                        PARAM_GROUP, 3, ConfigDef.Width.SHORT, "Password")

                .define(PARAM_COLLECTION, ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Kinetica collection name (optional, default--no collection name)",
                        PARAM_GROUP, 4, ConfigDef.Width.LONG, "Collection Name")

                .define(PARAM_TABLE_PREFIX, ConfigDef.Type.STRING, "",
                        ConfigDef.Importance.HIGH,
                        "Prefix applied to tablenames from Kafka schema. (optional)",
                        PARAM_GROUP, 5, ConfigDef.Width.LONG, "Table Prefix")

                .define(PARAM_DEST_TABLE_OVERRIDE, ConfigDef.Type.STRING, "",
                        ConfigDef.Importance.HIGH,
                        "Table name that will replace name automatically generated from the schema. (optional)",
                        PARAM_GROUP, 6, ConfigDef.Width.LONG, "Table Override")

                .define(PARAM_TIMEOUT, ConfigDef.Type.INT, DEFAULT_TIMEOUT, Range.atLeast(0),
                        ConfigDef.Importance.LOW,
                        "Kinetica timeout (ms) (optional, default " + DEFAULT_TIMEOUT + "); 0 = no timeout",
                        PARAM_GROUP, 7, ConfigDef.Width.SHORT, "Connection Timeout")

                .define(PARAM_BATCH_SIZE, ConfigDef.Type.INT, DEFAULT_BATCH_SIZE, Range.atLeast(1),
                        ConfigDef.Importance.LOW,
                        "Kinetica batch size (optional, default " + DEFAULT_BATCH_SIZE + ")",
                        PARAM_GROUP, 8, ConfigDef.Width.SHORT, "Batch Size")

                .define(PARAM_CREATE_TABLE, ConfigDef.Type.BOOLEAN, true,
                        ConfigDef.Importance.LOW,
                        "Create missing tables. (optional, default true)",
                        PARAM_GROUP, 9, ConfigDef.Width.SHORT, "Create Table");

    private final Map<String, String> config = new HashMap<>();

	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		Map<String,Object> configParsed = KineticaSinkConnector.CONFIG_DEF.parse(props);
		for (Map.Entry<String, Object> entry : configParsed.entrySet()) {
			this.config.put(entry.getKey(), entry.getValue().toString());
		}

		String connectUrl = props.get(KineticaSinkConnector.PARAM_URL);
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
        return(KineticaSinkConnector.CONFIG_DEF);
    }

	@Override
	public Class<? extends Task> taskClass() {
		return KineticaSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		List<Map<String, String>> taskConfigs = new ArrayList<>();

		for (int i = 0; i < maxTasks; i++) {
			taskConfigs.add(new HashMap<>(this.config));
		}
		return taskConfigs;
	}
}
