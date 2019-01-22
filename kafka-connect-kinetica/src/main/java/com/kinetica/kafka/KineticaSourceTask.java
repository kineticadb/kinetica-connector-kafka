package com.kinetica.kafka;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase;
import com.gpudb.GPUdbException;
import com.gpudb.protocol.ShowTableRequest;
import com.gpudb.protocol.ShowTableResponse;

/**
 * Kafka SourceTask for streaming data from a kinetica table.
 *
 * The data streaming pipeline will begin with creating a table monitor on the
 * given source table.  As records are inserted into the table, a copy will be
 * placed on a queue, to which the {@link KineticaSourceConnector} is attached.
 * The SourceTask will stream records from the queue as they are added, and add
 * them to a Kafka topic.
 *
 * The streaming source table can either be part of a collection or not, but
 * cannot be a collection itself.
 */
public class KineticaSourceTask extends SourceTask {
    private static final String CONF_MONITOR_PORT = "conf.set_monitor_port";
    private static final Logger LOG = LoggerFactory.getLogger(KineticaSourceTask.class);
    private static final int SCHEMA_VERSION = 1;

    private final LinkedBlockingQueue<SourceRecord> queue = new LinkedBlockingQueue<>();
    private final List<Thread> monitorThreads = new ArrayList<>();
    private String[] sourceTables;
    private int[] schemaVersions;
    private String zmqUrl;
    private String topicPrefix;

    public String getZmqUrl() {
        return this.zmqUrl;
    }

    public LinkedBlockingQueue<SourceRecord> getQueue() {
        return this.queue;
    }

    public String getTopicPrefix()  {
        return this.topicPrefix;
    }

    @Override
    public void start(final Map<String, String> props) {
        GPUdb gpudb;

        try {
            gpudb = getGpudb(props);
            this.zmqUrl = getZmqUrl(gpudb);
            this.topicPrefix = props.get(KineticaSourceConnectorConfig.PARAM_TOPIC_PREFIX);
            this.sourceTables = props.get(KineticaSourceConnectorConfig.PARAM_TABLE_NAMES).split(",");
            
            
            // Kinetica table can support only one single version of Kafka schema at a time
            this.schemaVersions = new int[this.sourceTables.length];
            if (props.containsKey(KineticaSourceConnectorConfig.PARAM_SCHEMA_VERSION) && 
            props.get(KineticaSourceConnectorConfig.PARAM_SCHEMA_VERSION)!=null ) {
            String[] split = props.get(KineticaSourceConnectorConfig.PARAM_SCHEMA_VERSION).split(",");
            for (int i = 0; i < split.length; i++) {
            this.schemaVersions[i] = split[i].isEmpty() ? 1 : Integer.parseInt(split[i]); 
            }
            } else {
            java.util.Arrays.fill(this.schemaVersions, 1);
            }

            for (String table : this.sourceTables) {
                // Get table info from /show/table.

                ShowTableResponse tableInfo = gpudb.showTable(table,
                        GPUdbBase.options(ShowTableRequest.Options.SHOW_CHILDREN, ShowTableRequest.Options.FALSE));

                // If the specified table is a collection, fail.
                if (tableInfo.getTableDescriptions().get(0).contains(ShowTableResponse.TableDescriptions.COLLECTION)) {
                    throw new ConnectException("Cannot create connector for collection " + table + ".");
                }
            }
        }
        catch (Exception ex) {
            throw new ConnectException(ex);
        }

        // Create a thread for each table that will manage the table monitor and
        // convert records from the monitor into source records and put them
        // into the source record queue. 

        for (int i = 0; i < this.sourceTables.length; i++) {
        final String table = this.sourceTables[i];
            // Assign pre-configured supported schema version for existing Kafka topics 
            // or set a default schema version value (1) for Kafka topic to be created on demand 
            KineticaMonitorThread monitorThread = new KineticaMonitorThread(this, gpudb, table, this.schemaVersions[i]);
            monitorThread.start();
            this.monitorThreads.add(monitorThread);
        }
    }

    /**
     * Terminates the monitor thread 
     */
    @Override
    public void stop() {
        // Interrupt the monitor threads and wait for them to terminate.
        for (Thread monitorThread : this.monitorThreads) {
            monitorThread.interrupt();
        }

        for (Thread monitorThread : this.monitorThreads) {
            try {
                monitorThread.join();
            } catch (InterruptedException ex) {
            }
        }
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    /**
     * Initiates queue messages processing
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // Copy any source records in the source record queue to the result;
        // block to wait for source records if none are present.

        List<SourceRecord> result = new ArrayList<>();

        if (this.queue.isEmpty())
        {
            result.add(this.queue.take());
        }

        this.queue.drainTo(result, 1000);
        LOG.info("Sourced " + result.size() + " records");

        return result;
    }

    /**
     * Retrieves table monitor URL from gpudb instance  
     * @param gpudb   Kinetica DB access object
     * @return table monitor URL
     * @throws GPUdbException
     * @throws MalformedURLException
     */
    private static String getZmqUrl(GPUdb gpudb) throws GPUdbException, MalformedURLException {
        // Get the table monitor URL from /show/system/properties. If table
        // monitor support is not enabled or the port is invalid, fail.
        String zmqPortString = gpudb.showSystemProperties(GPUdbBase.options()).getPropertyMap().get(CONF_MONITOR_PORT);

        if (zmqPortString == null || zmqPortString.equals("-1")) {
            throw new ConnectException("Table monitor not supported.");
        }

        int zmqPort;
        try {
            zmqPort = Integer.parseInt(zmqPortString);
        }
        catch (Exception ex) {
            throw new ConnectException("Invalid table monitor port (" + zmqPortString + ").");
        }

        if (zmqPort < 1 || zmqPort > 65535) {
            throw new ConnectException("Invalid table monitor port (" + zmqPortString + ").");
        }

        String hostname = gpudb.getURL().getHost();
        String zmqUrl = String.format("tcp://%s:%d", hostname, zmqPort);
        LOG.info("Got ZMQ URL <{}>", zmqUrl);

        return zmqUrl;
    }

    /**
     * Creates a connection to GPUdb and returns Kinetica DB access object
     * @param props                Connector config params
     * @return Kinetica DB access object
     * @throws GPUdbException 
     * @throws MalformedURLException
     */
    private static GPUdb getGpudb(Map<String, String> props) throws GPUdbException, MalformedURLException {
        final URL url = new URL(props.get(KineticaSourceConnectorConfig.PARAM_URL));

        int timeout = Integer.parseInt(props.get(KineticaSourceConnectorConfig.PARAM_TIMEOUT));
        String user = props.get(KineticaSourceConnectorConfig.PARAM_USERNAME);
        String passwd = props.get(KineticaSourceConnectorConfig.PARAM_PASSWORD);

        LOG.info("Connecting to <{}> as <{}>", url, user);
        
        LOG.debug(" UID/PWD " + user + "/" + passwd);
        
        GPUdb gpudb = new GPUdb(url, new GPUdb.Options()
                .setUsername(user)
                .setPassword(passwd)
                .setTimeout(timeout));
        return gpudb;
    }

}
