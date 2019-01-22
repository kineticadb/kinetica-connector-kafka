package com.kinetica.kafka;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
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

    private static final Logger LOG = LoggerFactory.getLogger(KineticaSourceConnector.class);
    
    private final Map<String, String> configProps = new HashMap<>();
    
    private KineticaSourceConnectorConfig connectorConfig;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {

        try {
            connectorConfig = new KineticaSourceConnectorConfig(props);
        } catch (ConfigException ce) {
            LOG.error("Couldn't start KineticaSourceConnector:", ce);
            throw new ConnectException("Couldn't start KineticaSourceConnector due to configuration error.", ce);
        } catch (Exception ce) {
            LOG.error("Couldn't start KineticaSourceConnector:", ce);
            throw new ConnectException("An error has occurred when starting KineticaSourceConnector" + ce);
        }

        for (Map.Entry<String, Object> entry : connectorConfig.config.parse(props).entrySet()) {
            this.configProps.put(entry.getKey(), (entry.getValue() != null ? entry.getValue().toString() : ""));
        }
        String connectUrl = props.get(KineticaSourceConnectorConfig.PARAM_URL);
        try {
            new URL(connectUrl);
        } catch (MalformedURLException ex) {
            throw new ConnectException("Invalid URL (" + connectUrl + ").");
        }
/*        
        if (this.context.inputsChanged()) {
            this.context.requestTaskReconfiguration();
        }*/

    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return KineticaSourceConnectorConfig.baseConfigDef();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return KineticaSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (connectorConfig == null) {
            throw new ConnectException("Connector config has not been initialized");
        }
        if (maxTasks <= 0) {
            throw new ConnectException("Invalid configuration, maximum task number must be a positive integer.");
        }
        List<String> tables = Arrays.asList(configProps.get(KineticaSourceConnectorConfig.PARAM_TABLE_NAMES).split(","));

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

            Map<String, String> taskConfig = new HashMap<>(configProps);
            taskConfig.put(KineticaSourceConnectorConfig.PARAM_TABLE_NAMES, tableNames);
            taskConfigs.add(taskConfig);
        }

        return taskConfigs;
    }
    
    
    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        return super.validate(connectorConfigs);
    }
}