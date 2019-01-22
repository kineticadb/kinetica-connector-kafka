package com.kinetica.kafka;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka SinkConnector for streaming data to a GPUdb table.
 *
 * The SinkConnector is used to configure the {@link KineticaSinkTask}, which
 * performs the work of writing data from Kafka into the target table.
 */
public class KineticaSinkConnector extends SinkConnector {
    
    private static final Logger LOG = LoggerFactory.getLogger(KineticaSinkConnector.class);

    private final Map<String, String> configProps = new HashMap<>();
    
    private KineticaSinkConnectorConfig connectorConfig;


    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            connectorConfig = new KineticaSinkConnectorConfig(props);
        } catch (ConfigException ce) {
            LOG.error("Couldn't start KineticaSinkConnector:", ce);
            throw new ConnectException("Couldn't start KineticaSinkConnector due to configuration error.", ce);
        } catch (Exception ce) {
            LOG.error("Couldn't start KineticaSinkConnector:", ce);
            throw new ConnectException("An error has occurred when starting KineticaSinkConnector" + ce);
        }
        
        for (Map.Entry<String, Object> entry : connectorConfig.config.parse(props).entrySet()) {
            this.configProps.put(entry.getKey(), (entry.getValue() != null ? entry.getValue().toString() : ""));
        }
        String connectUrl = props.get(KineticaSinkConnectorConfig.PARAM_URL);
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
        return KineticaSinkConnectorConfig.baseConfigDef();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return KineticaSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (connectorConfig == null) {
            throw new ConnectException("Connector config has not been initialized");
        }
        if (maxTasks<=0) {
            throw new ConnectException("Invalid configuration, maximum task number must be a positive integer.");
        }
        
        List<Map<String, String>> taskConfigs = new ArrayList<>();

        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(new HashMap<>(this.configProps));
        }
        return taskConfigs;
    }
    
    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        return super.validate(connectorConfigs);
    }
}
