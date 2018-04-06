package com.kinetica.kafka;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpudb.BulkInserter;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase;
import com.gpudb.GPUdbException;
import com.gpudb.GenericRecord;
import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.gpudb.protocol.CreateTableRequest;
import com.gpudb.protocol.InsertRecordsRequest;

public class SinkSchemaManager {

    private static final Logger LOG = LoggerFactory.getLogger(SinkSchemaManager.class);

    // from configuration
    private int batchSize;
    private final String tablePrefix;
    private final String tableOverride;
    private final String collectionName;
    private final GPUdb gpudb;
    private final boolean createTable;

    public SinkSchemaManager(Map<String, String> props) {
        this.batchSize = Integer.parseInt(props.get(KineticaSinkConnector.PARAM_BATCH_SIZE));
        this.tablePrefix = props.get(KineticaSinkConnector.PARAM_TABLE_PREFIX);
        this.tableOverride = props.get(KineticaSinkConnector.PARAM_DEST_TABLE_OVERRIDE);
        this.collectionName = props.get(KineticaSinkConnector.PARAM_COLLECTION);
        this.createTable = Boolean.parseBoolean(props.get(KineticaSinkConnector.PARAM_CREATE_TABLE));

        String url = props.get(KineticaSinkConnector.PARAM_URL);
        try {
            this.gpudb = new GPUdb(url, new GPUdb.Options()
                    .setUsername(props.get(KineticaSinkConnector.PARAM_USERNAME))
                    .setPassword(props.get(KineticaSinkConnector.PARAM_PASSWORD))
                    .setTimeout(Integer.parseInt(props.get(KineticaSinkConnector.PARAM_TIMEOUT))));
        }
        catch (GPUdbException ex) {
            ConnectException cex = new ConnectException("Unable to connect to Kinetica at: " + url, ex);
            LOG.error(cex.getMessage(), ex);
            throw cex;
        }
    }

    public BulkInserter<GenericRecord> getBulkInserter(String tableName, Type gpudbSchema) throws GPUdbException {
        HashMap<String,String> options = new HashMap<>();
        options.put(InsertRecordsRequest.Options.UPDATE_ON_EXISTING_PK, InsertRecordsRequest.Options.TRUE);
        return new BulkInserter<>(this.gpudb, tableName, gpudbSchema, this.batchSize, options);
    }


    public String getDestTable(String inputTable) throws Exception {
        if(!this.tableOverride.isEmpty()) {
            return this.tableOverride;
        }

        if(inputTable == null) {
            throw new Exception("Could not determine a table name from the record.");
        }

        String resultTable = inputTable;

        if(inputTable.contains(".")) {
            // table has a schema part so remove it.
            resultTable = inputTable.split("\\.")[1];
        }

        // generate the table from the schema
        return this.tablePrefix + resultTable;
    }

    public Type getType(String tableName, Object schema) throws Exception {
        Type gpudbType = null;

        if (this.gpudb.hasTable(tableName, null).getTableExists()) {
            // If the table exists, get the schema from it.
            LOG.info("Creating type for table: {}", tableName);
            gpudbType = Type.fromTable(this.gpudb, tableName);
            return gpudbType;
        }

        if(!this.createTable) {
            throw new Exception("Table does not exist and create_table=false: " + tableName);
        }

        if(schema instanceof Schema) {
            LOG.info("Converting type from Kafka Schema");
            gpudbType = convertTypeFromSchema((Schema)schema);
        }
        else if(schema instanceof HashMap) {
            LOG.info("Converting type from schema-less HashMap");
            @SuppressWarnings("unchecked")
            HashMap<String,Object> mapSchema = (HashMap<String,Object>)schema;
            gpudbType = convertTypeFromMap(mapSchema);
        }
        else {
            throw new Exception("Schema-less records must be a hashmap: " + schema.getClass().toString());
        }

        String typeId = gpudbType.create(this.gpudb);
        LOG.info("Creating table: {}", tableName);
        this.gpudb.createTable(tableName, typeId,
                GPUdbBase.options(CreateTableRequest.Options.COLLECTION_NAME, this.collectionName) );

        return gpudbType;
    }

    private Type convertTypeFromSchema(Schema kafkaSchema) throws Exception {
        // If the table does not exist, loop through the first sink record's
        // fields and build a type based on their names and types.
        List<Column> columns = new ArrayList<>();

        for (Field kafkaField : kafkaSchema.fields()) {
            Schema.Type kafkaType = kafkaField.schema().type();
            if(kafkaType == null) {
                continue;
            }

            switch (kafkaType) {
                case BYTES:
                    columns.add(new Column(kafkaField.name(), ByteBuffer.class));
                    break;

                case FLOAT64:
                    columns.add(new Column(kafkaField.name(), Double.class));
                    break;

                case FLOAT32:
                    columns.add(new Column(kafkaField.name(), Float.class));
                    break;

                case INT32:
                    columns.add(new Column(kafkaField.name(), Integer.class));
                    break;

                case INT64:
                    columns.add(new Column(kafkaField.name(), Long.class));
                    break;

                case STRING:
                    columns.add(new Column(kafkaField.name(), String.class));
                    break;

                default:
                    Exception ex = new Exception("Unsupported type for field " + kafkaField.name() + ".");
                    LOG.error(ex.getMessage());
                    throw ex;
            }
        }

        if (columns.isEmpty()) {
            throw new Exception("Schema has no fields.");
        }

        // Create the table based on the newly created schema.
        Type tableType = new Type(columns);
        return tableType;
    }

    private Type convertTypeFromMap(HashMap<String, Object> schema) throws Exception {
        List<Column> columns = new ArrayList<>();

        for(Map.Entry<String, Object> entry : schema.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if(value == null) {
                LOG.warn("Could not identify data type for column: <{}>", key);
                continue;
            }

            LOG.info("Adding column=<{}> Type=<{}>", key, value.getClass());
            columns.add(new Column(key, value.getClass(), "nullable"));
        }

        if (columns.isEmpty()) {
            throw new Exception("Schema has no fields.");
        }

        // Create the table based on the newly created schema.
        Type tableType = new Type(columns);
        return tableType;
    }

}
