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
	/** Config file key for Kinetica URL */
	public static final String URL_CONFIG = "kinetica.url";

	/** Config file key for Kinetica username */
	public static final String USERNAME_CONFIG = "kinetica.username";

	/** Config file key for Kinetica password */
	public static final String PASSWORD_CONFIG = "kinetica.password";

	/** Config file key for Kinetica request/response timeouts */
	public static final String TIMEOUT_CONFIG = "kinetica.timeout";

	/** Config file key for name of Kinetica collection containing target table */
	public static final String COLLECTION_NAME_CONFIG = "kinetica.collection_name";

	/** Config file key for name of Kinetica table to use as streaming target */
	//public static final String TABLE_NAME_CONFIG = "kinetica.table_name";

	/** String to prepend to table names from the message schema. */
	public static final String TABLE_PREFIX_CONFIG = "kinetica.table_prefix";

	/** Config file key for # of records to collect before writing to Kinetica */
	public static final String BATCH_SIZE_CONFIG = "kinetica.batch_size";


	public static final String DEFAULT_TIMEOUT = "0";
	public static final String DEFAULT_BATCH_SIZE = "10000";

	private Map<String, String> config;
	public static ConfigDef CONFIG_DEF =  new ConfigDef()
				.define(URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Kinetica URL, e.g. 'http://localhost:9191'","Kinetica Properties",1,ConfigDef.Width.SHORT,"Kinetica URL")
				.define(TIMEOUT_CONFIG, ConfigDef.Type.INT, DEFAULT_TIMEOUT, Range.atLeast(0),ConfigDef.Importance.HIGH, "Kinetica timeout (ms) (optional, default " + DEFAULT_TIMEOUT + "); 0 = no timeout","Kinetica Properties",2,ConfigDef.Width.SHORT,"Timeout")
				.define(COLLECTION_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Kinetica collection name (optional, default--no collection name)","Kinetica Properties",3,ConfigDef.Width.LONG,"Collection Name")
			   // .define(TABLE_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Kinetica table name","Kinetica Properties",4,ConfigDef.Width.LONG,"Table Name")
				.define(TABLE_PREFIX_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, "Kinetica table prefix","Kinetica Properties",4,ConfigDef.Width.LONG,"Table Prefix")
				.define(USERNAME_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Kinetica username (optional)","Kinetica Properties",5,ConfigDef.Width.SHORT,"Username")
				.define(PASSWORD_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Kinetica password (optional)","Kinetica Properties",6,ConfigDef.Width.SHORT,"Password")
				.define(BATCH_SIZE_CONFIG, ConfigDef.Type.INT, DEFAULT_BATCH_SIZE, Range.atLeast(1), ConfigDef.Importance.HIGH, "Kinetica batch size (optional, default " + DEFAULT_BATCH_SIZE + ")","Kinetica Properties",7,ConfigDef.Width.SHORT,"Batch Size");

	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		Map<String,Object> configParsed = KineticaSinkConnector.CONFIG_DEF.parse(props);
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
		return KineticaSinkTask.class;
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
		return(KineticaSinkConnector.CONFIG_DEF);
	}
}
