package com.kinetica.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpudb.BulkInserter;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase;
import com.gpudb.GPUdbException;
import com.gpudb.GenericRecord;
import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.gpudb.protocol.AlterTableColumnsRequest;
import com.gpudb.protocol.AlterTableRequest;
import com.gpudb.protocol.CreateTableRequest;
import com.gpudb.protocol.CreateTableResponse;
import com.gpudb.protocol.InsertRecordsRequest;

public class SinkSchemaManager {

    private static final Logger LOG = LoggerFactory.getLogger(SinkSchemaManager.class);

    // from configuration
    private int batchSize;
    protected final String tablePrefix;
    protected final String tableOverride;
    protected final String collectionName;
    protected final String topics;
    protected final GPUdb gpudb;
    protected final boolean createTable;
    protected final boolean addNewColumns;
    protected final boolean alterColumnsToNullable;
    protected final boolean singleTablePerTopic;
    protected final boolean allowSchemaEvolution;
    protected final boolean updateOnExistingPK;
    protected final boolean flattenSourceSchema;
    protected final String fieldNameDelimiter;
    protected final String arrayValueSeparator;
    protected final KineticaSinkConnectorConfig.ARRAY_FLATTENING arrayFlatteningMode;
    private final int retryCount;
    
    private final HashMap<String, List<Integer>> knownSchemas = new HashMap<>();
    private final HashMap<String, List<Integer>> blackListedSchemas = new HashMap<>();
    private final HashMap<String, HashMap<Integer, KineticaFieldMapper>> knownMappers = new HashMap<>();

    /**
     * Singleton schema management class. It parses schemas, when possible maps Kafka types to Kinetica avro types
     * validates incoming message schema against existing Kinetica types, 
     * keeps track of blacklisted schemas that failed mapping, etc.
     * @param props  Connector config properties to init the class
     */
    public SinkSchemaManager(Map<String, String> props) {
        for (String key : props.keySet()) {
            LOG.debug(key + " = " + props.get(key));
        }
        // Connector config options would be available to Task Workers only as SinkSchemaManager properties
        this.batchSize = Integer.parseInt( 
                props.get(KineticaSinkConnectorConfig.PARAM_BATCH_SIZE) );
        this.tablePrefix = 
                props.get(KineticaSinkConnectorConfig.PARAM_TABLE_PREFIX);
        this.tableOverride = 
                props.get(KineticaSinkConnectorConfig.PARAM_DEST_TABLE_OVERRIDE);
        this.collectionName = 
                props.get(KineticaSinkConnectorConfig.PARAM_COLLECTION);
        this.topics = 
                props.get( SinkTask.TOPICS_CONFIG );
        this.singleTablePerTopic = Boolean.parseBoolean( 
                props.get(KineticaSinkConnectorConfig.PARAM_SINGLE_TABLE_PER_TOPIC) );
        this.createTable = Boolean.parseBoolean(
                props.get(KineticaSinkConnectorConfig.PARAM_CREATE_TABLE) );
        this.addNewColumns = Boolean.parseBoolean(
                props.get(KineticaSinkConnectorConfig.PARAM_ADD_NEW_FIELDS) );
        this.alterColumnsToNullable = Boolean.parseBoolean( 
                props.get(KineticaSinkConnectorConfig.PARAM_MAKE_MISSING_FIELDS_NULLABLE) );
        this.retryCount = Integer.parseInt( 
                props.get(KineticaSinkConnectorConfig.PARAM_RETRY_COUNT).trim() );
        this.allowSchemaEvolution = Boolean.parseBoolean(
                props.get(KineticaSinkConnectorConfig.PARAM_ALLOW_SCHEMA_EVOLUTION) );
        this.updateOnExistingPK = Boolean.parseBoolean(
                props.get(KineticaSinkConnectorConfig.PARAM_UPDATE_ON_EXISTING_PK) );
        this.flattenSourceSchema = Boolean.parseBoolean(
                props.get(KineticaSinkConnectorConfig.PARAM_FLATTEN_SOURCE_SCHEMA) );
        
        if (props.containsKey(KineticaSinkConnectorConfig.PARAM_FIELD_NAME_DELIMITER)) {
        	this.fieldNameDelimiter = props.get(KineticaSinkConnectorConfig.PARAM_FIELD_NAME_DELIMITER);
        } else {
        	this.fieldNameDelimiter = KineticaSinkConnectorConfig.DEFAULT_FIELD_NAME_DELIMITER;
        }
        
        if (props.containsKey(KineticaSinkConnectorConfig.PARAM_ARRAY_VALUE_SEPARATOR)) {
        	this.arrayValueSeparator = props.get(KineticaSinkConnectorConfig.PARAM_ARRAY_VALUE_SEPARATOR);
        } else {
        	this.arrayValueSeparator = KineticaSinkConnectorConfig.DEFAULT_ARRAY_VALUE_SEPARATOR;
        }
        
        if (props.containsKey(KineticaSinkConnectorConfig.PARAM_ARRAY_FLATTENING_MODE) && 
        		props.get(KineticaSinkConnectorConfig.PARAM_ARRAY_FLATTENING_MODE) != null &&
        		!props.get(KineticaSinkConnectorConfig.PARAM_ARRAY_FLATTENING_MODE).isEmpty()) {
            this.arrayFlatteningMode = KineticaSinkConnectorConfig.ARRAY_FLATTENING.valueOf(props.get(KineticaSinkConnectorConfig.PARAM_ARRAY_FLATTENING_MODE));
        } else {
            this.arrayFlatteningMode = KineticaSinkConnectorConfig.ARRAY_FLATTENING.CONVERT_TO_STRING;
        }

        String url = props.get(KineticaSinkConnectorConfig.PARAM_URL);
        try {
            this.gpudb = new GPUdb(url, new GPUdb.Options()
                    .setUsername( props.get(KineticaSinkConnectorConfig.PARAM_USERNAME))
                    .setPassword( props.get(KineticaSinkConnectorConfig.PARAM_PASSWORD))
                    .setTimeout(Integer.parseInt( props.get(KineticaSinkConnectorConfig.PARAM_TIMEOUT))));
        }
        catch (GPUdbException ex) {
            ConnectException cex = new ConnectException("Unable to connect to Kinetica at: " + url, ex);
            LOG.error(cex.getMessage(), ex);
            throw cex;
        }
    }

    /**
     * Create a new BulkInserter for a given Kinetica tablename and gpudbSchema
     * @param  tableName      Kinetica tablename
     * @param  gpudbSchema    gpudb schema type
     * @return BulkInserter  for this Kinetica table
     * @throws GPUdbException
     */
    public BulkInserter<GenericRecord> getBulkInserter(String tableName, Type gpudbSchema) throws GPUdbException {
        HashMap<String,String> options = new HashMap<>();
        options.put(InsertRecordsRequest.Options.UPDATE_ON_EXISTING_PK, 
                (this.updateOnExistingPK ? InsertRecordsRequest.Options.TRUE : InsertRecordsRequest.Options.FALSE));
        BulkInserter<GenericRecord> result = new BulkInserter<>(this.gpudb, tableName, gpudbSchema, this.batchSize, options);
        result.setRetryCount(this.retryCount);
        return result;
    }
    
    public boolean isSingleTablePerTopic() {
        return this.singleTablePerTopic;
    }

    /**
     * Generate a Kinetica table name based on Connector configuration and incoming class type 
     * @param topic          Kafka topic name
     * @param inputSchema    record schema/class type
     * @return               Kinetica table name
     * @throws Exception
     */
    public String getDestTable(String topic, String inputSchema) throws Exception {
        // override tablename is not empty
        if(this.tableOverride!=null && !this.tableOverride.isEmpty()) {
            // many topics to single table override, destination table name is well-formed
            if (!this.singleTablePerTopic && !this.tableOverride.contains(",") )
                return this.tableOverride;
            // override tablename is not a list and topics not a list
            if (!this.tableOverride.contains(",") 
                    && !this.topics.contains(",")){
                return this.tableOverride;
            } else if (this.topics.split(",").length == this.tableOverride.split(",").length){
                for (int i=0; i<this.topics.split(",").length; i++) {
                    // find the topic position in config, then find a corresponding override name
                    if (topic.trim().equalsIgnoreCase(this.topics.split(",")[i].trim())) {
                        return this.tableOverride.split(",")[i];
                    }
                }
                // no topic name matched current topic
                throw new ConnectException("Could not determine an override table name for the topic from config params.");  
            }
        }
        
        // Clean up prefix
        String prefix = (this.tablePrefix != null && !this.tablePrefix.isEmpty()) ? this.tablePrefix.trim() : "";
        
        // no override defined, determine table name from topic/schema and prefix
        if (this.singleTablePerTopic) {
            // single table per topic
            LOG.debug("Single table per topic");
            // if topic is undefined, take simple name of schema object
            if (topic == null || topic.isEmpty()) {
                return prefix + getSimpleName(inputSchema);
            }
            // otherwise name table after topic
            return prefix + topic;
        } else {
            // for multiple tables per topic
            LOG.debug("Multiple tables per topic objects " + inputSchema);
            if(inputSchema == null) {
                LOG.error("Could not determine table name from Schema, generating table name from topic.");
                return prefix + topic;
            }
        
            // generate the table from the schema
            return prefix + getSimpleName(inputSchema);
        }
        
    }
    
    /**
     * Remove package portion of class name
     * @param fullName  full class name
     * @return String   simple class name 
     */
    private String getSimpleName(String fullName) {
        if(fullName!= null && fullName.contains(".")) {
            // name is dot-separated, remove the schema part
            String[] parts = fullName.split("\\.");
            return parts[parts.length-1];
        } else {
            return fullName;
        }
        
    }
    
    /**
     * Check if this version of schema for this table failed field mapping to Kinetica table  
     * and was added to the map of blacklisted schemas
     * @param tableName      Kinetica table name
     * @param genericSchema  Kafka record object 
     * @return true when the schema was blacklisted
     */
    public boolean isBlackListed(String tableName, Object genericSchema) {
        Integer version = versionOf(genericSchema);
        if (!this.allowSchemaEvolution  && knownSchemas.containsKey(tableName) 
                && !knownSchemas.get(tableName).contains(version)) {
            // Schema evolution is not allowed, but new version of schema is found
            // blacklist the schema version
            if (!blackListedSchemas.containsKey(tableName)) {
                blackListedSchemas.put(tableName, new ArrayList<Integer>());
            }
            blackListedSchemas.get(tableName).add(version);
        }
        if (!blackListedSchemas.containsKey(tableName)) {
            // when tablename is not in blackListed map, schema is not blacklisted
            return false; 
        } else 
            // tablename is found in blacklisted map
            if (!blackListedSchemas.get(tableName).contains(version)) {
                // when tablename is known, but its version is unknown, schema is not blacklisted
                return false;
        }
        return true;
    }

    /**
     * Adds schema that failed field mapping to Kinetica table to the map of blacklisted schemas  
     * @param tableName      Kinetica table name
     * @param genericSchema  Kafka record object
     */
    public void addToBlackListed(String tableName, Object genericSchema) {
        Integer version = versionOf(genericSchema);
        if (!blackListedSchemas.containsKey(tableName)) {
            // if tablename not recognized, put it into map, create a blank list of Integer schema versions 
            // to populate with schema versions (null is a valid version value)
            blackListedSchemas.put(tableName, new ArrayList<Integer>());
        }
        // add the version to the list of blacklisted schema versions
        blackListedSchemas.get(tableName).add(version);
        // remove blacklisted schema version from "whitelist" of knownSchemas
        if (knownSchemas.containsKey(tableName) && knownSchemas.get(tableName).contains(version)) {
            knownSchemas.get(tableName).remove(version);
        }
    }
    
    /**
     * Checks if incoming record needs field-by-field mapping to Kinetica table
     * Field mapping is an expensive operation and is avoided when possible. 
     * @param tableName      Kinetica table name
     * @param genericSchema  Kafka record object
     * @return true only when it's a new version of a known schema
     */
    protected boolean needsSchemaMapping(String tableName, Object genericSchema) {
        // when underlying Kinetica table can't be modified, no schema mapping between message formats is needed
        // data that does not fit existing format is ignored
        if (!this.allowSchemaEvolution || !this.singleTablePerTopic || !this.createTable)
            return false;
        
        Integer version = versionOf(genericSchema);

        LOG.debug("needsSchemaMapping started for [" + tableName + " " + version + "] " + (genericSchema!=null ? genericSchema.getClass().getName() : "null"));
        // if this tablename has never been seen before
        if (!knownSchemas.containsKey(tableName)) {
            // entirely new tablename should be added to knownSchemas map, no schema/table columns mapping needed            
            LOG.debug("entirely new schema");
            addToKnownSchemas(tableName, version);
            return false;
        } else {
            if (knownSchemas.get(tableName).contains(version)) {
                // known schema, known version, mapping has been performed
                LOG.debug("known schema, known version");
                return false;
            } else {
                // known schema, new version, need to map fields to columns 
                // in order to validate Kafka schema evolution
                LOG.debug("new version of a known schema");
                return true;
            }
        }        

    }

    /**
     * Checks if incoming object type is in the knownSchemas collection  
     * @param tableName      Kinetica table name
     * @param genericSchema  Kafka record object
     * @return true if it's a known schema
     */
    protected boolean isKnownSchema(String tableName, Object genericSchema) {
        Integer version = versionOf(genericSchema);
        if (knownSchemas.containsKey(tableName) && knownSchemas.get(tableName).contains(version)) {
            // known tablename, known version
            return true;
        } else {
            //unknown tablename OR new version of existing tablename
            return false;
        } 
    }
    
    /**
     * Extracts schema version from the Kafka record object
     * @param genericSchema   Kafka record object
     * @return version number or null for unversioned objects
     */
    public Integer versionOf(Object genericSchema) {
        if (genericSchema == null || genericSchema instanceof HashMap)
            return null;

        if (genericSchema instanceof Schema)
            return ((Schema)genericSchema).version();
        
        return null;
    }
    
    /**
     * Adds tablename/version combination to the knownSchemas collection
     * @param tableName      Kinetica table name
     * @param version        Kafka topic version
     */
    private void addToKnownSchemas(String tableName, Integer version) {
        if (!knownSchemas.containsKey(tableName)) {
            // add a new table to the map of table names
            knownSchemas.put(tableName, new ArrayList<Integer>());
        }
        // add version to the list of table schema versions
        knownSchemas.get(tableName).add(version);
    }

    /**
     * Extracts version from record object and adds tablename/version combination to the knownSchemas collection
     * @param tableName      Kinetica table name
     * @param genericSchema  Kafka record object
     */
    public void addToKnownSchemas(String tableName, Object genericSchema) {
        // determine the version
        Integer version = versionOf(genericSchema);
        // add this version to the list of schema versions
        addToKnownSchemas(tableName, version);
    }

    /**
     * Finds or creates a matching gpudbType for the incoming Kafka record object 
     * @param tableName      Kinetica table name
     * @param schema         incoming Kafka record object
     * @return gpudbType of existing Kinetica table
     * @throws Exception
     */
    public Type getType(String tableName, Object schema) throws Exception {
        
        Type gpudbType = null;
        boolean kineticaTableExists;
        
        try {
            gpudbType = Type.fromTable(this.gpudb, tableName);
            kineticaTableExists = true;

            LOG.debug("Found type for table: {}", tableName);            
        
        } catch (GPUdbException e) {
            kineticaTableExists = false;
            LOG.debug("Kinetica table {} does not exist, type not found", tableName);
        }
        // extract schema version
        Integer version = versionOf(schema);
        
        if (!kineticaTableExists) {
            // table does not exist and can't be created
            if(!this.createTable) {
                throw new ConnectException(String.format("Table %s does not exist and config option %s=false ", 
                        tableName, KineticaSinkConnectorConfig.PARAM_CREATE_TABLE)) ;
            }
            
            KineticaFieldMapper mapper = addBlankFieldMapper(tableName, version);

            if(schema instanceof Schema) {
                LOG.debug("Kafka schema");
                // extract list of columns for new table from Kafka schema
                LOG.debug("Converting type from Kafka Schema for table: {}", tableName);
                if (this.flattenSourceSchema) { 
                    gpudbType = KineticaTypeConverter.convertNestedTypeFromSchema(
                        (Schema) schema, 
                        this.fieldNameDelimiter, 
                        this.arrayFlatteningMode, 
                        this.arrayValueSeparator,
                        mapper);
                } else {
                    gpudbType = KineticaTypeConverter.convertTypeFromSchema((Schema) schema, mapper);            
                }
            } 
            else if(schema instanceof org.apache.avro.Schema) {
                LOG.debug("Avro schema");
                // extract list of columns for new table from avro schema
                LOG.debug("Converting type from Avro Schema for table: {}", tableName);
                if (this.flattenSourceSchema) { 
                    gpudbType = KineticaTypeConverter.convertNestedTypeFromAvroSchema(
                        (org.apache.avro.Schema) schema,
                        this.fieldNameDelimiter, 
                        this.arrayFlatteningMode, 
                        this.arrayValueSeparator,
                        mapper);
                } else {
                    gpudbType = KineticaTypeConverter.convertTypeFromAvroSchema((org.apache.avro.Schema) schema, mapper);
                }
            } 
            else if (schema instanceof HashMap) {
                LOG.debug("Schemaless");
                // extract list of columns for new table from column name/value hashmap
                LOG.debug("Converting type from schema-less HashMap");
                @SuppressWarnings("unchecked")
                HashMap<String,Object> mapSchema = (HashMap<String,Object>)schema;
                // No nested types can be parsed at this point, due to WorkerSinkTask 
                // throwing an error for badly formed JSON earlier than KineticaSinkTask is called.
                // Apply schema flattening of schemaless JSON by adding transform to connector config:
                // transforms=Flatten
                // transforms.Flatten.type=org.apache.kafka.connect.transforms.Flatten$Value
                // transforms.Flatten.delimiter=_
                gpudbType = KineticaTypeConverter.convertTypeFromMap(mapSchema, mapper);
                
            }
            else {
                // schema is missing, incoming object of unknown type, can't extract columns 
                throw new ConnectException("Schema-less records must be a hashmap: " + schema.getClass().toString());
            }
            
            // Create a type, then create table
            String typeId = gpudbType.create(this.gpudb);
            LOG.info("Creating table: {}", tableName);
            this.gpudb.createTable(tableName, typeId,
                        GPUdbBase.options(CreateTableRequest.Options.COLLECTION_NAME, this.collectionName) );
            // and add table name to recognized schemas
            addToKnownSchemas(tableName, schema);
        }
        
        if (!knownMappers.containsKey(tableName) || !knownMappers.get(tableName).containsKey(version)) {
            // for a brand new table create a mapper
            addFieldMapper(tableName, version, gpudbType);
        } 
        //mapAllFields(tableName, version, gpudbType);

        return gpudbType;
    }


    /**
     * Compares Kinetica table to Kafka record schema and creates a list of AlterTableRequests when
     * Connector configuration allows it
     * @param tableName       Kinetica table name
     * @param genericSchema   incoming Kafka record object
     * @param cachedType
     * @return List of AlterTableRequests to update underlying Kinetica table
     * @throws Exception
     */
    protected AlterTableColumnsRequest matchSchemas(String tableName, Object genericSchema, Type cachedType) throws Exception {
        LOG.debug("matchSchemas started for " + tableName);
        Integer version = versionOf(genericSchema);
        KineticaFieldMapper mapper = getFieldMapper(tableName, version);
        
        AlterTableColumnsRequest result = new AlterTableColumnsRequest(tableName, null, null);
        result.getOptions().put(AlterTableRequest.Options.COLUMN_PROPERTIES,  com.gpudb.ColumnProperty.NULLABLE);
        Type incoming;
        if (genericSchema instanceof Schema) {
            if (this.flattenSourceSchema) {
                incoming = KineticaTypeConverter.convertNestedTypeFromSchema((Schema)genericSchema, 
                    this.fieldNameDelimiter, 
                    this.arrayFlatteningMode, 
                    this.arrayValueSeparator, mapper);
            } else {
                incoming = KineticaTypeConverter.convertTypeFromSchema((Schema)genericSchema, mapper);
            }
        } else if (genericSchema instanceof org.apache.avro.Schema) {
            org.apache.avro.Schema writer = (org.apache.avro.Schema) genericSchema;
            if (this.flattenSourceSchema) {
                incoming = KineticaTypeConverter.convertNestedTypeFromAvroSchema(
                        writer, 
                        this.fieldNameDelimiter, 
                        this.arrayFlatteningMode, 
                        this.arrayValueSeparator, 
                        mapper);
            } else {
                incoming = KineticaTypeConverter.convertTypeFromAvroSchema(writer, mapper);
            }
        } else if (genericSchema instanceof HashMap) {
            HashMap<String, Object> writer = (HashMap<String, Object>) genericSchema;
            incoming = KineticaTypeConverter.convertTypeFromMap(writer, mapper);
        } else {
            throw new ConnectException("Unsupported schema type " + genericSchema.getClass() + ", schema match failed.");
        }

        HashMap<String, Column> existingFields = new HashMap<String, Column>();        
        HashMap<String, Column> incomingFields = new HashMap<String, Column>();
        
        for (Column column : cachedType.getColumns()) {
            existingFields.put(column.getName(), column);
        }
        
        // make sure that all incoming fields exist in Kinetica table or create a request to add column (when config allows it)
        for (Column incomingField : incoming.getColumns()) {
            boolean newField = true;
            incomingFields.put(incomingField.getName(), incomingField);
            if (existingFields.containsKey(incomingField.getName())){
                newField = false;
                mapper.getMapped().put(incomingField.getName(), incomingField);
            } 
            
            if (newField) {
                if (this.addNewColumns) {
                    LOG.info("Altering table " + tableName + " to add column " + incomingField.getName() 
                            + " of type " + incomingField.getType().getSimpleName());
                    
                    Map<String, String> alterations = new HashMap<String, String>();
                    alterations.put(AlterTableRequest.Options.ACTION, AlterTableRequest.Action.ADD_COLUMN);
                    alterations.put(AlterTableRequest.Options.COLUMN_NAME, incomingField.getName());
                    alterations.put(AlterTableRequest.Options.COLUMN_TYPE, KineticaTypeConverter.getKineticaColumnType(incomingField));
                    alterations.put(AlterTableRequest.Options.COLUMN_PROPERTIES, com.gpudb.ColumnProperty.NULLABLE);
                    String val = getFieldDefaultValue(incomingField.getName(), genericSchema);
                    if (val != null) {
                        alterations.put(AlterTableRequest.Options.COLUMN_DEFAULT_VALUE, val);
                    }

                    result.getColumnAlterations().add(alterations);
                    
                } else {
                    // warn that this record field is not recognized and its value is ignored because no changes to Kinetica table allowed 
                    LOG.warn("When ingesting data Column " + incomingField.getName() + " of Type " + incomingField.getType().getTypeName() 
                            + " is going to be ignored because configuration settings don't allow modifying table " + tableName 
                            + " through Connector. You can update table through gAdmin tool.");
                }
            }
            
        }

        // make sure all Kinetica required fields are present in incoming record or alter column to make it nullable (when config allows it)
        for (Column existingField : cachedType.getColumns()) {
            boolean missingField = true;
            if (existingField.isNullable() || incomingFields.containsKey(existingField.getName())){
                missingField = false;
            } 
            if (missingField) {
                if (this.alterColumnsToNullable){
                    LOG.info("Altering table " + tableName + " column " + existingField.getName() + " to make it nullable");
                    mapper.getMissing().put(existingField.getName(), existingField);
                    
                    StringBuilder sb = new StringBuilder(com.gpudb.ColumnProperty.NULLABLE);
                    for (String property : existingField.getProperties()) {
                        if (property != null && !property.isEmpty())
                            sb.append("," + property);
                    }
                    
                    Map<String, String> alterations = new HashMap<String, String>();
                    alterations.put(AlterTableRequest.Options.ACTION, AlterTableRequest.Action.CHANGE_COLUMN);
                    alterations.put(AlterTableRequest.Options.COLUMN_NAME, existingField.getName());
                    alterations.put(AlterTableRequest.Options.COLUMN_TYPE, KineticaTypeConverter.getKineticaColumnType(existingField));
                    alterations.put(AlterTableRequest.Options.COLUMN_PROPERTIES, sb.toString());

                    result.getColumnAlterations().add(alterations);
                    
                } else {
                    LOG.warn("During data ingest the required Column " + existingField.getName() + " of Type " + existingField.getType().getTypeName() 
                            + " was missing, and this record is going to be ignored. Configuration settings don't allow modifying columns of the table " + tableName 
                            + " through Connector. You can modify Column " + existingField.getName() + " to make it nullable through gAdmin tool.");
                    // blacklist the schema, because a required field is missing and no changes to Kinetica table allowed 
                    this.addToBlackListed(tableName, genericSchema);
                }
            }
        }
        // for all previous mappers of this table update field list, adding unknown fields to Missing collection
        updateFieldMappers(tableName, version, mapper);

        return result;
    }

    /**
     * Apply a batch of alterTableRequests to Kinetica table, wait until the table update completes 
     * then return the type of modified table  
     * @param tableName          underlying Kinetica table name
     * @param alterTableRequests a list of alter requests, executed one by one 
     * @return Type              record type of the newly modified table
     * @throws GPUdbException
     */
    
    public Type alterTable(String tableName, AlterTableColumnsRequest request) throws GPUdbException {
        try {
            this.gpudb.alterTableColumns(request);
            
        } catch (GPUdbException e) {
            LOG.error("AlterTableColumns exception: " + e.getMessage() + e.getStackTrace());
            return Type.fromTable(this.gpudb, tableName);
        }
        return Type.fromTable(this.gpudb, tableName);
    }

    /**
     * Lookup the FieldMapper by table name and version in the cache
     * @param tableName  Kinetica table name
     * @param version    Kafka record schema version
     * @return           KineticaFieldMapper
     */
    public KineticaFieldMapper getFieldMapper(String tableName, Integer version) {
        if (!knownMappers.containsKey(tableName) || !knownMappers.get(tableName).containsKey(version)) {
            // create a blank KineticaFieldMapper for unrecognized table/version combination 
            return addBlankFieldMapper(tableName, version);
        }
        // return existing KineticaFieldMapper from the cache
        return knownMappers.get(tableName).get(version);
    }
    
    /**
     * Add all the columns from gpudbType to the mapped columns of cached KineticaFieldMapper for this tablename/version
     * then update all previous KineticaFieldMappers of this table
     * @param tableName  Kinetica table name
     * @param version    Kafka record schema version
     * @param gpudbType  Kinetica table record type
     */
    public void mapAllFields(String tableName, Integer version, Type gpudbType) {
        KineticaFieldMapper mapper = getFieldMapper(tableName, version);
        for (Column col : gpudbType.getColumns()) {
            mapper.getMapped().put(col.getName(), col);
            mapper.getLookup().put(col.getName(), col.getName());
        }
        updateFieldMappers(tableName, version, mapper);    
    }
    
    /**
     * Create a new KineticaFieldMapper for this table name and schema version 
     * and populate it with fields from type
     * @param tableName  Kinetica table name
     * @param version    Kafka record schema version
     * @param gpudbType  Kinetica table record type
     * @return
     */
    private void addFieldMapper(String tableName, Integer version, Type gpudbType) {
        KineticaFieldMapper mapper = addBlankFieldMapper(tableName, version);
        for (Column col : gpudbType.getColumns()) {
            mapper.getMapped().put(col.getName(), col);
            mapper.getLookup().put(col.getName(), col.getName());
        }
        updateFieldMappers(tableName, version, mapper);
    }
    
    /**
     * Create a new KineticaFieldMapper for this table name and schema version 
     * @param tableName  Kinetica table name
     * @param version    Kafka record schema version
     * @return
     */
    private KineticaFieldMapper addBlankFieldMapper(String tableName, Integer version) {
        if (!knownMappers.containsKey(tableName)) {
            knownMappers.put(tableName, new HashMap<Integer,KineticaFieldMapper>());
        }
        if (!knownMappers.get(tableName).containsKey(version)) {            
            knownMappers.get(tableName).put(version, new KineticaFieldMapper(tableName, version));
        }
        return knownMappers.get(tableName).get(version);
    }
    
    /**
     * Loop through all previous KineticaFieldMappers for the given table name and version and add
     * fields unknown to previous mappers to their collections of missing fields 
     * @param tableName  Kinetica table name
     * @param version    Kafka record schema version
     * @param mapper     KineticaFieldMapper for this tablename/version
     */
    public void updateFieldMappers(String tableName, Integer version, KineticaFieldMapper mapper) {
        
        if (knownMappers.get(tableName)!=null) {
            LOG.debug("Found Previous mappers for [tableName=" + tableName + "] " + knownMappers.get(tableName).size());
            // compare all old mappers with the most recent mapper up-to-date with current gpudbtype 
            for (KineticaFieldMapper old : knownMappers.get(tableName).values()) {
                LOG.debug("mapper for [tableName=" + old.getTableName() + "] " + old.getVersion());
                // for all columns mapped in most recent mapper
                for (String colName : mapper.getMapped().keySet()) {
                    // if a column is missing from both mapped and missing  
                    if (!old.getMapped().keySet().contains(colName) && !old.getMissing().keySet().contains(colName)) {
                        old.getMissing().put(colName, mapper.getMapped().get(colName));
                    }
                }
                // for all columns missing from most recent mapper
                for (String colName : mapper.getMissing().keySet()) {
                    // if a column is missing from both mapped and missing  
                    // columns of old mapper, add it to missing columns
                    if (!old.getMapped().keySet().contains(colName) && !old.getMissing().keySet().contains(colName)) {
                        old.getMissing().put(colName, mapper.getMissing().get(colName));
                    }
                }
            }
        } 
    }
    
    /**
     * Retrieves most recent Type for the given table (in case table was modified at the time of execution) 
     * @param tableName
     * @return  gpudbType for the table
     * @throws GPUdbException
     */
    public Type getGpudbType(String tableName) throws GPUdbException {
        return Type.fromTable(this.gpudb, tableName);
    }
    
    protected String getFieldDefaultValue(String name, Object genericSchema) {
        try {
            if (genericSchema instanceof Schema) {
                org.apache.kafka.connect.data.Field field = ((Schema)genericSchema).field(name);
                return (String)field.schema().defaultValue();
            } 
            if (genericSchema instanceof org.apache.avro.Schema) {
                org.apache.avro.Schema.Field field = ((org.apache.avro.Schema) genericSchema).getField(name);
                return (String)field.defaultVal();
            }
            return null;
        } catch (Exception e) {
            LOG.error(e.getMessage() + e.getStackTrace());
            return null;
        }
    }
}
