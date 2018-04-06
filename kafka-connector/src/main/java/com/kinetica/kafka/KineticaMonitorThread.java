package com.kinetica.kafka;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import com.gpudb.Avro;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.GenericRecord;
import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.gpudb.protocol.CreateTableMonitorResponse;

public class KineticaMonitorThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(KineticaMonitorThread.class);

    // control if record values should have a Schema
    private final boolean schemalessValues = false;

    private final GPUdb gpudb;
    private final String kineticaTable;
    private final String kafkaTopic;
    private final String zmqUrl;
    private final LinkedBlockingQueue<SourceRecord> queue;

    private Type kineticaType;
    private Schema kafkaSchema;
    private String zmqTopic;

    public KineticaMonitorThread(KineticaSourceTask source, GPUdb gpudb, String table) {
        this.gpudb = gpudb;
        this.kafkaTopic = source.getTopicPrefix() + table;
        this.kineticaTable = table;
        this.zmqUrl = source.getZmqUrl();
        this.queue = source.getQueue();
    }


    @Override
    public void run() {
        LOG.info("Starting monitor thread for table <{}>", this.kineticaTable);

        try {
            // Create the table monitor.
            CreateTableMonitorResponse response = this.gpudb.createTableMonitor(this.kineticaTable, null);
            this.kineticaType = new Type(response.getTypeSchema());
            this.zmqTopic = response.getTopicId();

            // Create a Kafka schema from the table type.
            this.kafkaSchema = getSchema(this.kineticaTable, this.kineticaType);

            // Subscribe to the ZMQ topic for the table monitor.
            try (ZMQ.Context zmqContext = ZMQ.context(1); ZMQ.Socket subscriber = zmqContext.socket(ZMQ.SUB)) {
                subscriber.connect(this.zmqUrl);
                subscriber.subscribe(this.zmqTopic.getBytes());
                subscriber.setReceiveTimeOut(1000);

                LOG.info("Starting ZMQ monitor ID=<{}> table=<{}>", this.zmqTopic, this.kineticaTable);
                processTopic(subscriber);
            }

        }
        catch (GPUdbException ex) {
            String msg = String.format("Error processing monitor <%s> for table <%s>: %s",
                    this.zmqUrl, this.kineticaTable, ex.getMessage());
            throw new ConnectException(msg, ex);
        }

        // separate try block to prevent resource leak
        try {
            LOG.info("Stopping ZMQ monitor ID=<{}> table=<{}>", this.zmqTopic, this.kineticaTable);
            this.gpudb.clearTableMonitor(this.zmqTopic, null);
        }
        catch (GPUdbException ex) {
            throw new ConnectException(ex);
        }
    }

    private static Schema getSchema(String tableName, Type type)  {
        SchemaBuilder builder = SchemaBuilder.struct().name(tableName).version(1);

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
                throw new KafkaException("Unknown column type " + column.getType().getName() + ".");
            }

            builder = builder.field(column.getName(), fieldSchema);
        }
        return builder.build();
    }

    private void processTopic(Socket subscriber) throws GPUdbException {
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
            LOG.info("Got ZMQ message ID=<{}> table=<{}>", this.zmqTopic, this.kineticaTable);

            boolean skip = true;
            for (ZFrame frame : message) {
                // Skip the first frame (which just contains the
                // topic ID).

                if (skip) {
                    skip = false;
                    continue;
                }
                processFrame(frame, recordNumber++);
            }
        }
    }

    private void processFrame(ZFrame frame, long recordNumber) throws GPUdbException {
        // Create a source record for each record and put it
        // into the source record queue.

        GenericRecord inRecord = Avro.decode(this.kineticaType, ByteBuffer.wrap(frame.getData()));
        final Object valueData;
        final Schema valueSchema;
        final Object keyData;
        final Schema keySchema;

        if(this.schemalessValues) {
            // follow the GoldenGate convention and use the tablename with no schema
            keySchema = null;
            keyData = this.kineticaTable;
            valueSchema = null;
            valueData = getValueMap(inRecord);
        }
        else {
            // follow the Kinetica convention and give the key a sequence with a schema
            keySchema = Schema.INT64_SCHEMA;
            keyData = new Long(recordNumber);
            valueSchema = this.kafkaSchema;
            valueData = getValueStruct(inRecord);
        }

        Map<String,?> sourcePartition = Collections.singletonMap("table", this.kineticaTable);
        Map<String,?> sourceOffset = Collections.singletonMap("record", recordNumber);

        SourceRecord outRecord = new SourceRecord(
                sourcePartition, sourceOffset,
                this.kafkaTopic,
                keySchema, keyData,
                valueSchema, valueData);

        this.queue.add(outRecord);
    }

    private HashMap<String, Object> getValueMap(GenericRecord inRecord) {
        HashMap<String, Object>  valueMap = new HashMap<>();

        for (Column column : this.kineticaType.getColumns()) {
            Object value = inRecord.get(column.getName());
            valueMap.put(column.getName(), value);
        }

        return valueMap;
    }

    private Struct getValueStruct(GenericRecord inRecord) {
        Struct valueStruct = new Struct(this.kafkaSchema);

        for (Column column : this.kineticaType.getColumns()) {
            Object value = inRecord.get(column.getName());

            if (value instanceof ByteBuffer) {
                value = ((ByteBuffer)value).array();
            }
            valueStruct.put(column.getName(), value);
        }

        return valueStruct;
    }
}
