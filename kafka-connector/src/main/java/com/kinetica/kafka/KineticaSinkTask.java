package com.kinetica.kafka;

import com.gpudb.BulkInserter;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.GenericRecord;
import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.gpudb.protocol.CreateTableRequest;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka SinkTask for streaming data into a Kinetica table.
 *
 * The data streaming pipeline will begin with records being added to the Kafka
 * topic to which the {@link KineticaSinkConnector} is attached.  As records are
 * queued, this SinkTask will connect to that queue, stream records from it, and
 * insert them into a Kinetica target table.
 *
 * The streaming target table can either be part of a collection or not, and can
 * also be a collection itself.
 */
public class KineticaSinkTask extends SinkTask {
    private static final Logger LOG = LoggerFactory.getLogger(KineticaSinkTask.class);

    private Map<String, String> config;
    private Type type;
    private BulkInserter<GenericRecord> bi;

    @Override
    public void start(Map<String, String> props) {
        config = new HashMap<>(props);
    }
    
    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        if (sinkRecords.isEmpty()) {
            return;
        }

        String url = config.get(KineticaSinkConnector.URL_CONFIG);
        String tableName = config.get(KineticaSinkConnector.TABLE_NAME_CONFIG);

        // If there is no bulk inserter yet (because this is the first time put
        // was called), either look up the type from the specified table (if it
        // exists) or create one (and the table) by analyzing the first sink
        // record in the collection, then create the bulk inserter.

        if (bi == null) {
            GPUdb gpudb;

            try {
                
                gpudb = new GPUdb(url, new GPUdb.Options()
                        .setUsername(config.get(KineticaSinkConnector.USERNAME_CONFIG))
                        .setPassword(config.get(KineticaSinkConnector.PASSWORD_CONFIG))
                        .setTimeout(Integer.parseInt(config.get(KineticaSinkConnector.TIMEOUT_CONFIG))));
            } catch (GPUdbException ex) {
                LOG.error("Unable to connect to Kinetica at " + url, ex);
                throw new ConnectException(ex);
            }

            // If the table exists, get the schema from it.

            try {
                if (gpudb.hasTable(tableName, null).getTableExists()) {
                    type = Type.fromTable(gpudb, tableName);
                }
            } catch (GPUdbException ex) {
                LOG.error("Unable to lookup table " + tableName + " in Kinetica at " + url, ex);
                throw new ConnectException(ex);
            }

            // If the table does not exist, loop through the first sink record's
            // fields and build a type based on their names and types.

            if (type == null) {
                Schema schema = sinkRecords.iterator().next().valueSchema();
                List<Column> columns = new ArrayList<>();

                try {
                    for (Field field : schema.fields()) {
                        if (null != field.schema().type()) switch (field.schema().type()) {
                            case BYTES:
                                columns.add(new Column(field.name(), ByteBuffer.class));
                                break;

                            case FLOAT64:
                                columns.add(new Column(field.name(), Double.class));
                                break;

                            case FLOAT32:
                                columns.add(new Column(field.name(), Float.class));
                                break;

                            case INT32:
                                columns.add(new Column(field.name(), Integer.class));
                                break;

                            case INT64:
                                columns.add(new Column(field.name(), Long.class));
                                break;

                            case STRING:
                                columns.add(new Column(field.name(), String.class));
                                break;

                            default:
                            	String errorMessage = "Unsupported type for field " + field.name() + ".";
                                LOG.error(errorMessage);
                                throw new ConnectException(errorMessage);
                        }
                    }
                } catch (DataException ex) {
                    LOG.error("Schema is not a struct.");
                    return;
                }

                if (columns.isEmpty()) {
                    LOG.error("Schema has no fields.");
                    return;
                }

                // Create the table based on the newly created schema.

                try {
                    type = new Type(columns);
                    gpudb.createTable
                    (tableName,
                        type.create(gpudb),
                        GPUdb.options(CreateTableRequest.Options.COLLECTION_NAME, config.get(KineticaSinkConnector.COLLECTION_NAME_CONFIG))
                    );
                } catch (Exception ex) {
                    LOG.error("Unable to create table " + tableName + " in Kinetica at " + url, ex);
                    throw new ConnectException(ex);
                }
            }

            // Create the bulk inserter based on the previously obtained type.

            try {
                bi = new BulkInserter<>(gpudb,
                        config.get(KineticaSinkConnector.TABLE_NAME_CONFIG),
                        type,
                        Integer.parseInt(config.get(KineticaSinkConnector.BATCH_SIZE_CONFIG)),
                        null);
            } catch (GPUdbException ex) {
                LOG.error("Unable to create bulk inserter for table " + tableName + " in Kinetica at " + url, ex);
                throw new ConnectException(ex);
            }
        }

        // Loop through all sink records in the collection and create records
        // out of them.

        for (SinkRecord sinkRecord : sinkRecords) {
            Struct struct;

            try {
                struct = (Struct)sinkRecord.value();
            } catch (Exception ex) {
                LOG.error("Schema is not a struct.");
                continue;
            }

            GenericRecord record = new GenericRecord(type);
            boolean fail = false;

            // Loop through all columns.

            for (Column column : type.getColumns()) {
                String columnName = column.getName();

                // Check the type of the field in the sink record versus the
                // type of the column to ensure compatibility; if not
                // compatible, fail the record, otherwise copy the value.

                Object value;

                try {
                    value = struct.get(columnName);
                } catch (Exception ex) {
                    LOG.error("Column " + columnName + " is missing.");
                    fail = true;
                    break;
                }

                if (value == null) {
                    LOG.error("Unsupported null value in field " + columnName + ".");
                    fail = true;
                    break;
                }

                Class<?> columnType = column.getType();

                if (columnType == ByteBuffer.class) {
                    if (value instanceof byte[]) {
                        value = ByteBuffer.wrap((byte[])value);
                    } else {
                        fail = true;
                    }
                } else if (columnType == Double.class) {
                    if (!(value instanceof Double)) {
                        fail = true;
                    }
                } else if (columnType == Float.class) {
                    if (!(value instanceof Float)) {
                        fail = true;
                    }
                } else if (columnType == Integer.class) {
                    if (!(value instanceof Integer)) {
                        fail = true;
                    }
                } else if (columnType == Long.class) {
                    if (!(value instanceof Long)) {
                        fail = true;
                    }
                } else if (columnType == String.class) {
                    if (!(value instanceof String)) {
                        fail = true;
                    }
                }

                if (fail) {
                    LOG.error("Type mismatch in field " + columnName + ".");
                    break;
                }

                record.put(columnName, value);
            }

            // If the record did not fail, check to make sure all columns in the
            // schema are accounted for, and if not, fail the record.

            if (!fail) {
                for (int i = 0; i < type.getColumnCount(); i++) {
                    if (record.get(i) == null) {
                        LOG.error("Missing field " + type.getColumn(i).getName() + ".");
                        fail = true;
                        break;
                    }
                }
            }

            // If the record didn't fail, insert into GPUdb.

            if (!fail) {
                try {
                    bi.insert(record);
                } catch (GPUdbException ex) {
                    LOG.error("Unable to insert into table " + tableName + " in Kinetica at " + url, ex);
                    throw new ConnectException(ex);
                }
            }
        }

        LOG.info("Sunk " + sinkRecords.size() + " records");
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (bi != null) {
            try {
                bi.flush();
            } catch (GPUdbException ex) {
                LOG.error("Unable to insert into table " + config.get(KineticaSinkConnector.TABLE_NAME_CONFIG) + " in GPUdb at " + config.get(KineticaSinkConnector.URL_CONFIG), ex);
                throw new ConnectException(ex);
            }
        }
    }

    @Override
    public void stop() {
        bi = null;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}