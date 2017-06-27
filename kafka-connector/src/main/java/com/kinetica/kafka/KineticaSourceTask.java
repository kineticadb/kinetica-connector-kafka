package com.kinetica.kafka;

import com.gpudb.Avro;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.GenericRecord;
import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.gpudb.protocol.CreateTableMonitorResponse;
import com.gpudb.protocol.ShowTableRequest;
import com.gpudb.protocol.ShowTableResponse;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import org.apache.kafka.common.errors.InterruptException;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

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

    private LinkedBlockingQueue<SourceRecord> queue;
    private List<Thread> monitorThreads;

    @Override
    public void start(final Map<String, String> props) {
        final URL url;
        final String[] tables = props.get(KineticaSourceConnector.TABLE_NAMES_CONFIG).split(",");

        try {
            url = new URL(props.get(KineticaSourceConnector.URL_CONFIG));
        } catch (MalformedURLException ex) {
            throw new IllegalArgumentException("Invalid URL (" + props.get(KineticaSourceConnector.URL_CONFIG) + ").");
        }

        final GPUdb gpudb;
        final String zmqUrl;

        try {
            gpudb = new GPUdb(url, new GPUdb.Options()
                    .setUsername(props.get(KineticaSourceConnector.USERNAME_CONFIG))
                    .setPassword(props.get(KineticaSourceConnector.PASSWORD_CONFIG))
                    .setTimeout(Integer.parseInt(props.get(KineticaSourceConnector.TIMEOUT_CONFIG))));

            for (String table : tables) {
                // Get table info from /show/table.

                ShowTableResponse tableInfo = gpudb.showTable(table, GPUdb.options(ShowTableRequest.Options.SHOW_CHILDREN, ShowTableRequest.Options.FALSE));

                // If the specified table is a collection, fail.

                if (tableInfo.getTableDescriptions().get(0).contains(ShowTableResponse.TableDescriptions.COLLECTION)) {
                    throw new ConnectException("Cannot create connector for collection " + table + ".");
                }
            }

            // Get the table monitor URL from /show/system/properties. If table
            // monitor support is not enabled or the port is invalid, fail.

            String zmqPortString = gpudb.showSystemProperties(GPUdb.options()).getPropertyMap().get(CONF_MONITOR_PORT);

            if (zmqPortString == null || zmqPortString.equals("-1")) {
                throw new ConnectException("Table monitor not supported.");
            }

            int zmqPort;

            try {
                zmqPort = Integer.parseInt(zmqPortString);
            } catch (Exception ex) {
                throw new ConnectException("Invalid table monitor port (" + zmqPortString + ").");
            }

            if (zmqPort < 1 || zmqPort > 65535) {
                throw new ConnectException("Invalid table monitor port (" + zmqPortString + ").");
            }

            zmqUrl = "tcp://" + url.getHost() + ":" + zmqPort;
        } catch (GPUdbException ex) {
            throw new ConnectException(ex);
        }

        queue = new LinkedBlockingQueue<>();
        monitorThreads = new ArrayList<>();

        // Create a thread for each table that will manage the table monitor and
        // convert records from the monitor into source records and put them
        // into the source record queue.

        for (final String table : tables) {
            Thread monitorThread = new Thread() {
                @Override
                public void run() {
                    Type type;
                    String topicId;

                    try {
                        // Create the table monitor.

                        CreateTableMonitorResponse response = gpudb.createTableMonitor(table, null);
                        type = new Type(response.getTypeSchema());
                        topicId = response.getTopicId();
                    } catch (GPUdbException ex) {
                        LOG.error("Could not create table monitor for " + table + " at " + url + ".", ex);
                        throw new ConnectException(ex);
                    }

                    // Create a Kafka schema from the table type.

                    SchemaBuilder builder = SchemaBuilder.struct().name(table).version(1);

                    for (Column column : type.getColumns())
                    {
                        Schema fieldSchema;

                        if (column.getType() == ByteBuffer.class) {
                            fieldSchema = Schema.BYTES_SCHEMA;
                        } else if (column.getType() == Double.class) {
                            fieldSchema = Schema.FLOAT64_SCHEMA;
                        } else if (column.getType() == Float.class) {
                            fieldSchema = Schema.FLOAT32_SCHEMA;
                        } else if (column.getType() == Integer.class) {
                            fieldSchema = Schema.INT32_SCHEMA;
                        } else if (column.getType() == Long.class) {
                            fieldSchema = Schema.INT64_SCHEMA;
                        } else if (column.getType() == String.class) {
                            fieldSchema = Schema.STRING_SCHEMA;
                        } else {
                            LOG.error("Unknown column type " + column.getType().getName() + ".");
                            return;
                        }

                        builder = builder.field(column.getName(), fieldSchema);
                    }

                    Schema schema = builder.build();

                    // Subscribe to the ZMQ topic for the table monitor.

                    try (ZMQ.Context zmqContext = ZMQ.context(1); ZMQ.Socket subscriber = zmqContext.socket(ZMQ.SUB)) {
                        subscriber.connect(zmqUrl);
                        subscriber.subscribe(topicId.getBytes());
                        subscriber.setReceiveTimeOut(1000);

                        // Loop until the thread is interrupted when the task is
                        // stopped.

                        long recordNumber = 0;

                        while (!Thread.currentThread().isInterrupted()) {
                            // Check for a ZMQ message; if none was received within
                            // the timeout window continue waiting.

                            ZMsg message = ZMsg.recvMsg(subscriber);

                            if (message == null) {
                                continue;
                            }

                            boolean skip = true;

                            for (ZFrame frame : message) {
                                // Skip the first frame (which just contains the
                                // topic ID).

                                if (skip) {
                                    skip = false;
                                    continue;
                                }

                                // Create a source record for each record and put it
                                // into the source record queue.

                                GenericRecord record = Avro.decode(type, ByteBuffer.wrap(frame.getData()));
                                Struct struct = new Struct(schema);

                                for (Column column : type.getColumns()) {
                                    Object value = record.get(column.getName());

                                    if (value instanceof ByteBuffer) {
                                        value = ((ByteBuffer)value).array();
                                    }

                                    struct.put(column.getName(), value);
                                }

                                queue.add(new SourceRecord(
                                        Collections.singletonMap("table", table),
                                        Collections.singletonMap("record", recordNumber),
                                        props.get(KineticaSourceConnector.TOPIC_PREFIX_CONFIG) + table,
                                        Schema.INT64_SCHEMA,
                                        recordNumber,
                                        schema,
                                        struct
                                ));

                                recordNumber++;
                            }
                        }
                    } catch (Exception ex) {
                        LOG.error("Could not access table monitor for " + table + " at " + zmqUrl + ".", ex);
                        throw new ConnectException(ex);
                    }

                    // The task has been stopped (or something failed) so clear the
                    // table monitor.

                    try {
                        gpudb.clearTableMonitor(topicId, null);
                    } catch (GPUdbException ex) {
                        LOG.error("Could not clear table monitor for " + table + " at " + url + ".", ex);
                        throw new ConnectException(ex);
                    }
                }
            };

            monitorThread.start();
            monitorThreads.add(monitorThread);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // Copy any source records in the source record queue to the result;
        // block to wait for source records if none are present.

        List<SourceRecord> result = new ArrayList<>();

        if (queue.isEmpty())
        {
            result.add(queue.take());
        }

        queue.drainTo(result, 1000);

        LOG.info("Sourced " + result.size() + " records");

        return result;
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void stop() {
        // Interrupt the monitor threads and wait for them to terminate.

        for (Thread monitorThread : monitorThreads) {
            monitorThread.interrupt();
        }

        for (Thread monitorThread : monitorThreads) {
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
}