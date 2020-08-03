package com.kinetica.kafka;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.gpudb.protocol.AlterTableRequest;
/**
 * A helper class, providing a collection of static methods for parsing Kafka schemas, Avro schemas, and Hashmap Objects
 * detecting and converting Types, mapping Java types to Schema Types, etc 
 *
 */
public class KineticaTypeConverter {
    /**
     * Builds Kinetica gpudbType based on incoming Kafka Schema 
     * @param kafkaSchema  incoming Kafka schema object
     * @return gpudbType for destination Kinetica table
     * @throws Exception
     */
    public static Type convertTypeFromSchema(Schema kafkaSchema) throws Exception {
        // loop through the Kafka schema fields to build a type based on names and data types.
        List<Column> columns = new ArrayList<>();
        
        if (isPrimitiveType(kafkaSchema)) {
            addColumn(kafkaSchema.type(), kafkaSchema.name(), columns, new ArrayList<String>());
        }

        for (Field kafkaField : kafkaSchema.fields()) {
            Schema.Type kafkaType = kafkaField.schema().type();
            if(kafkaType == null) {
                continue;
            }
            List<String> properties = new ArrayList<String>();
            if( kafkaField.schema().isOptional() ) {
                properties.add("nullable");
            }
            addColumn(kafkaField.schema().type(), kafkaField.name(), columns, properties);
        }

        if (columns.isEmpty()) {
            ConnectException ex = new ConnectException("Schema has no fields.");
            throw ex;
        }

        // Create the table type based on the newly created schema.
        Type tableType = new Type(columns);
        return tableType;
    }
    /**
     * Builds Kinetica gpudbType based on incoming Avro Schema 
     * @param avroSchema    incoming avro schema object
     * @return gpudbType for destination Kinetica table
     */
    public static Type convertTypeFromAvroSchema(org.apache.avro.Schema avroSchema) {
        // parse the attached Avro schema to build a type based on the Avro schema types.
        List<Column> columns = new ArrayList<>();

        switch(avroSchema.getType()) {
            // abort if top record is an unsupported complex type
            case ARRAY:
            case MAP:
            case UNION:
            case ENUM:
            case FIXED:
            case NULL:
                ConnectException ex = new ConnectException("Unsupported top-level type for schema " + avroSchema.getFullName() + ".");
                throw ex;
            // parse top-level RECORD by field, creating columns accordingly
            case RECORD:
                for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
                    parseAvroType(field.schema(), field.name(), columns, false);
                }
                break;
            // parse top-level primitive type, creating a column accordingly
            case BYTES:
            case FLOAT:
            case DOUBLE:
            case INT:
            case LONG:
            case STRING:
            case BOOLEAN:
                parseAvroType(avroSchema, avroSchema.getName(), columns, false);
            default:
                break;
        }

        if (columns.isEmpty()) {
            ConnectException ex = new ConnectException("Schema has no fields.");
            throw ex;
        }

        // Create the table type based on the newly created schema.
        Type tableType = new Type(columns);
          return tableType;
    }
    
    /**
     * Adds column to columns collection deriving Type and Type properties
     * for Kinetica-specific attributes from incoming avro schema for the field
     * @param avroSchema   incoming Avro Schema object
     * @param name         column name
     * @param columns      collection of column definitions
     * @param nullable     is column nullable
     */
    public static void parseAvroType(org.apache.avro.Schema avroSchema, String name, List<Column> columns, boolean nullable) {
        List<String> properties = new ArrayList<String>();
        if (nullable) {
            properties.add("nullable");
        }
        
        org.apache.avro.Schema.Type schemaType = avroSchema.getType();
        
        switch (schemaType) {
            // map primitive types as is:
            case BYTES:
                columns.add(new Column(name, ByteBuffer.class, properties));
                break;
    
            case FLOAT:
                columns.add(new Column(name, Float.class, properties));
                break;
    
            case DOUBLE:
                columns.add(new Column(name, Double.class, properties));
                break;
    
            case INT:
                columns.add(new Column(name, Integer.class, properties));
                break;
    
            case LONG:
                columns.add(new Column(name, Long.class, properties));
                break;
    
            case STRING:
                columns.add(new Column(name, String.class, properties));
                break;
    
            case BOOLEAN:
                // treat boolean type as a string column with 2 possible values "true" and "false"
                // this makes it a dictionary with character limit 8
                properties.add("dict");
                properties.add("char8");
                columns.add(new Column(name, String.class, properties));
                break;
                
            case FIXED:
                // treat fixed column as a bytes object
                properties.add("store_only");
                columns.add(new Column(name, ByteBuffer.class, properties));
                break;
                
            case UNION:
                // treat child-level UNION as a simple type with nullable value
                if (avroSchema.getTypes().size()==2 && isNullable(avroSchema)) {
                    // if field type is a Union consisting of exactly two types, a NULL and another type, create a column of the non-null type
                    for (org.apache.avro.Schema embeddedSchema : avroSchema.getTypes()) {
                        if (embeddedSchema.getType() != org.apache.avro.Schema.Type.NULL) {
                            // pass nullable property in a flag when parsing underlying schema type
                            parseAvroType(embeddedSchema, name, columns, true);
                        }
                    }
                }
                break;
                
            case ENUM:
                // treat child-level enum types as string columns with a constrained length, calculated to fit the longest enum value
                int maxSize = 0;
                for (String value : avroSchema.getEnumSymbols()) {
                    maxSize = Math.max(value.length(), maxSize);
                }
                if (maxSize>0 && maxSize <=256) {
                    properties.add(matchCharType(maxSize));
                }
                // this property is setting a dictionary restriction on a string column
                properties.add("dict");
                columns.add(new Column(name, String.class, properties));            
                break;
            
            default:
                ConnectException ex = new ConnectException("Unsupported type for field " + name + ".");
                throw ex;
            }
        
    }
    
    /**
     * Derives Kinetica string type attributes from field size
     * @param count    field character count
     * @return custom char attribute
     */
    public static String matchCharType(int count) {
        // for strings with limited length, match character count to a constrained string column type [char1, char2, char4, ..., char256]
        if (Integer.highestOneBit(count)==count)
            return "char" + count;
        else
            return "char" + 2*Integer.highestOneBit(count);
    }
    
    /**
     * Derives gpudbType from Hashmap field values
     * @param schema             incoming key-value pairs in a HashMap
     * @return gpudbType for destination Kinetica table
     * @throws ConnectException
     */
    public static Type convertTypeFromMap(HashMap<String, Object> schema) throws ConnectException {
        List<Column> columns = new ArrayList<>();

        for(Map.Entry<String, Object> entry : schema.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if(value == null) {
                continue;
            } else {
                Class<?> type = value.getClass();
                if (value instanceof byte[] || type.getSimpleName().contains("ByteBuffer")) {
                    type = ByteBuffer.class;
                }
                columns.add(new Column(key, type, "nullable"));
            }
        }

        if (columns.isEmpty()) {
            throw new ConnectException("Schema has no fields.");
        }

        // Create the table type based on the newly created schema.
        Type tableType = new Type(columns);
        return tableType;
    }

    /** 
     * Extracts primitive Type from provided avro Type, populating existing field options
     * with custom Kinetica attributes          
     * @param field     avro schema field
     * @param options   String collection of options
     * @return primitive avro Type
     */
    public static org.apache.avro.Schema.Type getUnderlyingType(org.apache.avro.Schema.Field field, Map<String, String> options) {
        if (isPrimitiveType(field.schema())) {
            return field.schema().getType();
        }
        if (isSupportedType(field.schema())) {
            switch (field.schema().getType()) {
                case FIXED:
                    options.put(AlterTableRequest.Options.COLUMN_PROPERTIES, "string"+(isNullable(field.schema())?",nullable":"" ));
                    return org.apache.avro.Schema.Type.BYTES;
                case ENUM:
                    options.put(AlterTableRequest.Options.COLUMN_PROPERTIES, "dict"+(isNullable(field.schema())?",nullable":"" ));
                    return org.apache.avro.Schema.Type.STRING;
                case BOOLEAN:
                    options.put(AlterTableRequest.Options.COLUMN_PROPERTIES, "dict,char8"+(isNullable(field.schema())?",nullable":"" ));
                    return org.apache.avro.Schema.Type.STRING;
                case UNION:
                    options.put(AlterTableRequest.Options.COLUMN_PROPERTIES, "nullable");
                    for (org.apache.avro.Schema unionSubType : field.schema().getTypes()) {
                        if (unionSubType.getType() == org.apache.avro.Schema.Type.NULL) {
                            continue;
                        } else {
                            return unionSubType.getType();
                        }
                    }
                default:
                    return field.schema().getType();
                }
        }
        throw new ConnectException("Unsupported type for field " + field.name() + ".");        
    }

    /**
     * Adds column to columns collection based on Kafka type and provided properties
     * @param kafkaType   Kafka Schema type provided in incoming data
     * @param name        column name
     * @param columns     columns collection 
     * @param properties  field properties
     * @throws Exception
     */
    public static void addColumn(Schema.Type kafkaType, String name, List<Column> columns, List<String> properties) throws Exception {
        switch (kafkaType) {
            case BYTES: 
                columns.add(new Column(name, ByteBuffer.class, properties));
                break;
            case FLOAT64: 
                columns.add(new Column(name, Double.class, properties));
                break;
            case FLOAT32: 
                columns.add(new Column(name, Float.class, properties));
                break;
            case INT32:  
                columns.add(new Column(name, Integer.class, properties));
                break;
            case INT64:  
                columns.add(new Column(name, Long.class, properties));
                break;
            case STRING:  
                columns.add(new Column(name, String.class, properties));
                break;
            default:
                // when the column type is not in the list of provided primitive types, 
                // Kafka schema is invalid and Kafka data can't be exported to Kinetica table 
                Exception ex = new ConnectException("Unsupported type for field " + name + ".");
                throw ex;
        }
    }

    /**
     * Checks if the provided schema defines a primitive Type
     * @param schema    incoming Kafka schema
     * @return true when the type is primitive
     */
    public static boolean isPrimitiveType(Schema schema) {
        Schema.Type type = schema.type();
        // returns true for primitive types only
        switch (type) {
            case BYTES:
                return true;
                
            case FLOAT32: 
            case FLOAT64:
                return true;

            // case BOOLEAN:

            case INT8: 
            case INT16: 
            case INT32: 
            case INT64:  
                return true;
            
            case STRING:
                return true;

            default: 
                return false;
        }
    }
    
    /**
     * Checks if the provided schema defines a primitive Type
     * @param schema    incoming Avro schema
     * @return true when the type is primitive
     */
    public static boolean isPrimitiveType(org.apache.avro.Schema schema) {
        org.apache.avro.Schema.Type type = schema.getType();
        switch (type) {
            case BYTES:
            case FLOAT:
            case DOUBLE:
            case INT:
            case LONG:
            case STRING:
            case FIXED:
                return true;
            default: 
                return false;
        }
    }
    
    /**
     * Checks if the provided schema defines a supported Type
     * @param schema    incoming Avro schema
     * @return true when the type is supported by Kinetica
     */

    public static boolean isSupportedType(org.apache.avro.Schema schema) {
        if (isPrimitiveType(schema))
            return true;

        org.apache.avro.Schema.Type type = schema.getType();
        switch (type) {
            // complex types are not allowed at child-level
            case ARRAY:
            case MAP:
            case RECORD:
                return false;
            case ENUM:
            case BOOLEAN:
                return true;
            case UNION:
                if (schema.getTypes()!= null) {
                    if (schema.getTypes().size()>2) {
                        return false;
                    }
                    for (org.apache.avro.Schema unionSubType : schema.getTypes()) {
                        if (unionSubType.getType() == org.apache.avro.Schema.Type.NULL) {
                            continue;
                        } else {
                            return isSupportedType(unionSubType);
                        }
                    }
                }
                return false;
            default:
                return false;
        }
    }

    /**
     * Checks if the provided schema defines a nullable Type
     * @param schema    incoming avro schema
     * @return true when the type is nullable
     */
    public static boolean isNullable(org.apache.avro.Schema schema) {
        // primitive types don't have an embedded union, not nullable
        if (isPrimitiveType(schema))
            return false;

        org.apache.avro.Schema.Type type = schema.getType();
        switch (type) {
            // nullable type by definition of NULL
            case NULL:
                return true;
            // could be a union of some type with nullable:
            case UNION:
                if (schema.getTypes()!= null && schema.getTypes().size()>0) {
                    // when union has multiple types joined
                    for (org.apache.avro.Schema unionSubType : schema.getTypes()) {
                        // if one of the types in nullable
                        if (unionSubType.getType() == org.apache.avro.Schema.Type.NULL) {
                            // return true
                            return true;
                        } else {
                            // otherwise proceed to check other types
                            continue;
                        }
                    }
                }
                // if no NULL definition found in union, source type is not nullable 
                return false;
            default:
                // all other complex types are not supported by Kinetica, and can't be nullable
                return false;
        }
    }
    
    /**
     * Derives Kinetica column type from string notation
     * @param column   Kinetica column type
     * @return
     */
    public static String getKineticaColumnType(Column column) {
        if (column.getType() == null)
            return "null";
        switch(column.getType().getSimpleName()) {
            case "Integer" : return "int";
            case "Double" :
            case "Float" :
            case "Long" :
            case "String":
                return column.getType().getSimpleName().toLowerCase();
            case "ByteBuffer":
                return "byte";
            default : return "null";
        }
    }


}
