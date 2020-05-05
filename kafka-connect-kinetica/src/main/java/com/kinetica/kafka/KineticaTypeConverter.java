package com.kinetica.kafka;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.gpudb.protocol.AlterTableRequest;

/**
 * A helper class, providing a collection of static methods for parsing Kafka
 * schemas, Avro schemas, and Hashmap Objects detecting and converting Types,
 * mapping Java types to Schema Types, etc
 *
 */
public class KineticaTypeConverter {
	private static final Logger LOG = LoggerFactory.getLogger(KineticaTypeConverter.class);

	// A special symbol not allowed in Kafka field names
	// Used in KineticaFieldMapper field keys to indicate nested type references
	// This symbol is replaced with configured fieldNameDelimiter when creating
	// Kinetica column name
	public static final String separator = ":";

	/**
	 * Builds Kinetica gpudbType based on incoming Kafka Schema
	 * 
	 * @param kafkaSchema incoming Kafka schema object
	 * @return gpudbType for destination Kinetica table
	 * @throws Exception
	 */
	public static Type convertTypeFromSchema(Schema kafkaSchema, KineticaFieldMapper mapper) throws Exception {
		// loop through the Kafka schema fields to build a type based on names and data
		// types.
		List<Column> columns = new ArrayList<>();

		if (isPrimitiveType(kafkaSchema)) {
			addColumn(kafkaSchema, kafkaSchema.type(), "", kafkaSchema.name(), columns, new ArrayList<String>(), null,
					null, null, mapper);
		}

		for (Field kafkaField : kafkaSchema.fields()) {
			Schema.Type kafkaType = kafkaField.schema().type();
			if (kafkaType == null) {
				continue;
			}
			List<String> properties = new ArrayList<String>();
			if (kafkaField.schema().isOptional()) {
				properties.add("nullable");
			}
			addColumn(kafkaSchema, kafkaField.schema().type(), "", kafkaField.name(), columns, properties, null, null,
					null, mapper);
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
	 * Flattens incoming nested Kafka Schema and builds Kinetica gpudbType for it
	 * 
	 * @param kafkaSchema incoming Kafka schema object
	 * @return gpudbType for destination Kinetica table
	 * @throws Exception
	 */
	public static Type convertNestedTypeFromSchema(Schema kafkaSchema, String fieldNameDelimiter,
			KineticaSinkConnectorConfig.ARRAY_FLATTENING arrayFlatteningMode, String arrayValueSeparator,
			KineticaFieldMapper mapper) throws Exception {
		// loop through the Kafka schema fields to build a type based on names and data
		// types.
		List<Column> columns = new ArrayList<>();
		if (isPrimitiveType(kafkaSchema)) {
			addColumn(kafkaSchema, kafkaSchema.type(), "", kafkaSchema.name(), columns, new ArrayList<String>(),
					fieldNameDelimiter, arrayFlatteningMode, arrayValueSeparator, mapper);
		} else {
			List<String> properties = new ArrayList<String>();
			properties.add("nullable");
			switch (kafkaSchema.type()) {
			case MAP: {
				Schema mapValueSchema = kafkaSchema.valueSchema();
				addColumn(mapValueSchema, mapValueSchema.type(), "", mapValueSchema.name(), columns,
						mapValueSchema.isOptional() ? properties : new ArrayList<String>(), fieldNameDelimiter,
						arrayFlatteningMode, arrayValueSeparator, mapper);
				break;
			}
			case ARRAY: {
				Schema arrItemSchema = kafkaSchema.valueSchema();
				switch (arrayFlatteningMode) {
				case CONVERT_TO_STRING: {
					addColumn(arrItemSchema, Schema.Type.STRING, "", kafkaSchema.name(), columns,
							kafkaSchema.isOptional() ? properties : new ArrayList<String>(), fieldNameDelimiter,
							arrayFlatteningMode, arrayValueSeparator, mapper);
					break;
				}
				default:
					break;
				}
				break;
			}
			case STRUCT: {
				for (Field kafkaField : kafkaSchema.fields()) {
					Schema.Type kafkaType = kafkaField.schema().type();
					if (kafkaField.schema() == null || kafkaType == null) {
						continue;
					}
					addColumn(kafkaField.schema(), kafkaField.schema().type(), "", kafkaField.name(), columns,
							kafkaField.schema().isOptional() ? properties : new ArrayList<String>(), fieldNameDelimiter,
							arrayFlatteningMode, arrayValueSeparator, mapper);
				}
				break;
			}
			default:
				ConnectException ex = new ConnectException("Unsupported top-level type '" + kafkaSchema.type()
						+ "' for schema " + kafkaSchema.name() + ".");
				throw ex;
			}
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
	 * 
	 * @param avroSchema incoming avro schema object
	 * @return gpudbType for destination Kinetica table
	 */
	public static Type convertTypeFromAvroSchema(org.apache.avro.Schema avroSchema, KineticaFieldMapper mapper) {
		// parse the attached Avro schema to build a type based on the Avro schema
		// types.
		List<Column> columns = new ArrayList<>();

		switch (avroSchema.getType()) {
		// Abort if top record is an unsupported complex type
		case ARRAY:
		case MAP:
		case UNION:
		case ENUM:
		case FIXED:
		case NULL:
			ConnectException ex = new ConnectException(
					"Unsupported top-level type for schema " + avroSchema.getFullName() + ".");
			throw ex;

			// Parse top-level RECORD by field, creating columns accordingly
		case RECORD:
			for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
				parseAvroType(field.schema(), "", field.name(), columns, false,
						KineticaSinkConnectorConfig.DEFAULT_FIELD_NAME_DELIMITER, mapper);
			}
			break;

		// Parse top-level primitive type, creating a column accordingly
		case BYTES:
		case FLOAT:
		case DOUBLE:
		case INT:
		case LONG:
		case STRING:
		case BOOLEAN:
			parseAvroType(avroSchema, "", avroSchema.getName(), columns, false,
					KineticaSinkConnectorConfig.DEFAULT_FIELD_NAME_DELIMITER, mapper);
			break;
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
	 * Flattens incoming nested Avro Schema and builds Kinetica gpudbType for it
	 * 
	 * @param avroSchema incoming avro schema object
	 * @return gpudbType for destination Kinetica table
	 */
	public static Type convertNestedTypeFromAvroSchema(org.apache.avro.Schema avroSchema, String fieldNameDelimiter,
			KineticaSinkConnectorConfig.ARRAY_FLATTENING arrayFlatteningMode, String arrayValueSeparator,
			KineticaFieldMapper mapper) {
		// parse the attached Avro schema to build a type based on the Avro schema
		// types.
		List<Column> columns = new ArrayList<>();
		switch (avroSchema.getType()) {
		// Abort if top record is an unsupported complex type
		case UNION:
		case ENUM:
		case FIXED:
		case NULL: {
			ConnectException ex = new ConnectException(
					"Unsupported top-level type for schema " + avroSchema.getFullName() + ".");
			throw ex;
		}
		// Parse top-level RECORD, ARRAY or MAP type by field, creating columns
		// accordingly
		case RECORD: {
			for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
				parseAvroType(field.schema(), "", field.name(), columns, false, fieldNameDelimiter, mapper);
			}
			break;
		}
		case ARRAY: {
			switch (arrayFlatteningMode) {
			case CONVERT_TO_STRING: {
				// Array types are to be treated as string values only.
				// during data ingest connector would convert all array values
				// to strings and concatenate with comma separator
				org.apache.avro.Schema elementShema = avroSchema.create(org.apache.avro.Schema.Type.STRING);
				parseAvroType(elementShema, "", avroSchema.getName(), columns, true, fieldNameDelimiter, mapper);
				break;
			}
			case TAKE_FIRST_VALUE: {
				org.apache.avro.Schema elementSchema = avroSchema.getValueType();
				parseAvroType(elementSchema, "", avroSchema.getName(), columns, true, fieldNameDelimiter, mapper);
				break;
			}
			default:
				break;
			}
			break;
		}
		case MAP: {
			org.apache.avro.Schema mapItemSchema = avroSchema.getValueType();
			for (String key : avroSchema.getObjectProps().keySet()) {
				Object obj = avroSchema.getObjectProp(key);
				parseAvroType(mapItemSchema, "", key, columns, true, fieldNameDelimiter, mapper);
			}
			break;
		}
		// Parse top-level primitive type, creating a column accordingly
		case BYTES:
		case FLOAT:
		case DOUBLE:
		case INT:
		case LONG:
		case STRING:
		case BOOLEAN: {
			parseAvroType(avroSchema, "", avroSchema.getName(), columns, false, fieldNameDelimiter, mapper);
			break;
		}
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
	 * Adds column to columns collection deriving Type and Type properties for
	 * Kinetica-specific attributes from incoming avro schema for the field
	 * 
	 * @param avroSchema incoming Avro Schema object
	 * @param name       column name
	 * @param columns    collection of column definitions
	 * @param nullable   is column nullable
	 */
	public static void parseAvroType(org.apache.avro.Schema avroSchema, String prefix, String name,
			List<Column> columns, boolean nullable, String fieldNameDelimiter, KineticaFieldMapper mapper) {
		List<String> properties = new ArrayList<String>();
		if (nullable) {
			properties.add("nullable");
		}

		org.apache.avro.Schema.Type schemaType = avroSchema.getType();
		Column column;
		String columnName;

		switch (schemaType) {
		// Parse supported complex types recursively
		case ARRAY: {
			// Array types are to be treated as string values only.
			// during data ingest connector would convert all array values
			// to strings and concatenate with comma separator
			org.apache.avro.Schema elementShema = avroSchema.create(org.apache.avro.Schema.Type.STRING);
			parseAvroType(elementShema, prefix, name, columns, true, fieldNameDelimiter, mapper);
			break;
		}
		case MAP: {
			org.apache.avro.Schema mapItemSchema = avroSchema.getValueType();
			parseAvroType(mapItemSchema, prefix, name, columns, true, fieldNameDelimiter, mapper);
			break;
		}
		case RECORD: {
			for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
				parseAvroType(field.schema(), (prefix.isEmpty() ? "" : prefix + separator) + name, field.name(),
						columns, false, fieldNameDelimiter, mapper);
			}
			break;
		}
		// Map primitive types as is:
		case BYTES: {
			columnName = (prefix.isEmpty() ? "" : prefix + separator) + name;
			column = new Column(columnName.replaceAll(separator, fieldNameDelimiter), ByteBuffer.class, properties);
			columns.add(column);
			mapper.getMapped().put(columnName, column);
			mapper.getLookup().put(column.getName(), columnName);
			LOG.debug("\t\t\t" + schemaType + " [" + columnName + " -> " + column.getName() + "]");
			break;
		}
		case FLOAT: {
			columnName = (prefix.isEmpty() ? "" : prefix + separator) + name;
			column = new Column(columnName.replaceAll(separator, fieldNameDelimiter), Float.class, properties);
			columns.add(column);
			mapper.getMapped().put(columnName, column);
			mapper.getLookup().put(column.getName(), columnName);
			LOG.debug("\t\t\t" + schemaType + " [" + columnName + " -> " + column.getName() + "]");
			break;
		}
		case DOUBLE: {
			columnName = (prefix.isEmpty() ? "" : prefix + separator) + name;
			column = new Column(columnName.replaceAll(separator, fieldNameDelimiter), Double.class, properties);
			columns.add(column);
			mapper.getMapped().put(columnName, column);
			mapper.getLookup().put(column.getName(), columnName);
			LOG.debug("\t\t\t" + schemaType + " [" + columnName + " -> " + column.getName() + "]");
			break;
		}
		case INT: {
			columnName = (prefix.isEmpty() ? "" : prefix + separator) + name;
			column = new Column(columnName.replaceAll(separator, fieldNameDelimiter), Integer.class, properties);
			columns.add(column);
			mapper.getMapped().put(columnName, column);
			mapper.getLookup().put(column.getName(), columnName);
			LOG.debug("\t\t\t" + schemaType + " [" + columnName + " -> " + column.getName() + "]");
			break;
		}
		case LONG: {
			columnName = (prefix.isEmpty() ? "" : prefix + separator) + name;
			column = new Column(columnName.replaceAll(separator, fieldNameDelimiter), Long.class, properties);
			columns.add(column);
			mapper.getMapped().put(columnName, column);
			mapper.getLookup().put(column.getName(), columnName);
			LOG.debug("\t\t\t" + schemaType + " [" + columnName + " -> " + column.getName() + "]");
			break;
		}
		case STRING: {
			columnName = (prefix.isEmpty() ? "" : prefix + separator) + name;
			column = new Column(columnName.replaceAll(separator, fieldNameDelimiter), String.class, properties);
			columns.add(column);
			mapper.getMapped().put(columnName, column);
			mapper.getLookup().put(column.getName(), columnName);
			LOG.debug("\t\t\t" + schemaType + " [" + columnName + " -> " + column.getName() + "]");
			break;
		}
		case BOOLEAN: {
			// treat boolean type as an integer column with possible values 1 and 0
			columnName = (prefix.isEmpty() ? "" : prefix + separator) + name;
			column = new Column(columnName.replaceAll(separator, fieldNameDelimiter), Integer.class, properties);
			columns.add(column);
			mapper.getMapped().put(columnName, column);
			mapper.getLookup().put(column.getName(), columnName);
			LOG.debug("\t\t\t" + schemaType + " [" + columnName + " -> " + column.getName() + "]");
			break;
		}
		case FIXED: {
			// treat fixed column as a bytes object
			columnName = (prefix.isEmpty() ? "" : prefix + separator) + name;
			properties.add("store_only");
			column = new Column(columnName.replaceAll(separator, fieldNameDelimiter), ByteBuffer.class, properties);
			columns.add(column);
			mapper.getMapped().put(columnName, column);
			mapper.getLookup().put(column.getName(), columnName);
			LOG.debug("\t\t\t" + schemaType + " [" + columnName + " -> " + column.getName() + "]");
			break;
		}
		case UNION: {
			// treat child-level UNION as a simple type with nullable value
			if (avroSchema.getTypes().size() == 2 && isNullable(avroSchema)) {
				// if field type is a Union consisting of exactly two types, a NULL and another
				// type, create a column of the non-null type
				for (org.apache.avro.Schema embeddedSchema : avroSchema.getTypes()) {
					if (embeddedSchema.getType() != org.apache.avro.Schema.Type.NULL) {
						// pass nullable property in a flag when parsing underlying schema type
						parseAvroType(embeddedSchema, prefix, name, columns, true, fieldNameDelimiter, mapper);
					}
				}
			}
			break;
		}
		case ENUM: {
			// treat child-level enum types as string columns with a constrained length,
			// calculated to fit the longest enum value
			int maxSize = 0;
			for (String value : avroSchema.getEnumSymbols()) {
				maxSize = Math.max(value.length(), maxSize);
			}
			if (maxSize > 0 && maxSize <= 256) {
				properties.add(matchCharType(maxSize));
			}
			// this property is setting a dictionary restriction on a string column
			columnName = (prefix.isEmpty() ? "" : prefix + separator) + name;
			properties.add("dict");
			column = new Column(columnName.replaceAll(separator, fieldNameDelimiter), String.class, properties);
			columns.add(column);
			mapper.getMapped().put(columnName, column);
			mapper.getLookup().put(column.getName(), columnName);
			LOG.debug("\t\t\t" + schemaType + " [" + columnName + " -> " + column.getName() + "]");
			break;
		}
		default:
			ConnectException ex = new ConnectException("Unsupported type for field " + name + ".");
			throw ex;
		}

	}

	/**
	 * Derives Kinetica string type attributes from field size
	 * 
	 * @param count field character count
	 * @return custom char attribute
	 */
	public static String matchCharType(int count) {
		// for strings with limited length, match character count to a constrained
		// string column type [char1, char2, char4, ..., char256]
		if (Integer.highestOneBit(count) == count)
			return "char" + count;
		else
			return "char" + 2 * Integer.highestOneBit(count);
	}

	/**
	 * Derives gpudbType from Hashmap field values
	 * 
	 * @param schema incoming key-value pairs in a HashMap
	 * @return gpudbType for destination Kinetica table
	 * @throws ConnectException
	 */
	public static Type convertTypeFromMap(HashMap<String, Object> schema, KineticaFieldMapper mapper)
			throws ConnectException {
		List<Column> columns = new ArrayList<>();

		for (Map.Entry<String, Object> entry : schema.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();

			if (value == null) {
				continue;
			} else {
				Column column;
				Class<?> type = value.getClass();
				String sName = value.getClass().getSimpleName();
				if (value instanceof byte[] || sName.contains("ByteBuffer")) {
					type = ByteBuffer.class;
				}
				if (value instanceof Boolean || sName.contains("Boolean")) {
					column = new Column(key, Integer.class, "nullable");
				} else {
					column = new Column(key, type, "nullable");
				}
				columns.add(column);
				mapper.getMapped().put(key, column);
				mapper.getLookup().put(column.getName(), key);

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
	 * Derives gpudbType from Hashmap field values
	 * 
	 * @param schema incoming key-value pairs in a HashMap
	 * @return gpudbType for destination Kinetica table
	 * @throws ConnectException
	 */
	public static Type convertNestedTypeFromMap(HashMap<String, Object> schema, KineticaFieldMapper mapper)
			throws ConnectException {
		List<Column> columns = new ArrayList<>();

		for (Map.Entry<String, Object> entry : schema.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();

			if (value == null) {
				continue;
			} else {
				Column column;
				Class<?> type = value.getClass();
				if (value instanceof byte[] || type.getSimpleName().contains("ByteBuffer")) {
					type = ByteBuffer.class;
				}
				if (value instanceof Boolean) {
					column = new Column(key, Integer.class, "nullable");
				} else {
					column = new Column(key, type, "nullable");
				}
				columns.add(column);
				mapper.getMapped().put(key, column);
				mapper.getLookup().put(column.getName(), key);
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
	 * Extracts primitive Type from provided avro Type, populating existing field
	 * options with custom Kinetica attributes
	 * 
	 * @param field   avro schema field
	 * @param options String collection of options
	 * @return primitive avro Type
	 */
	public static org.apache.avro.Schema.Type getUnderlyingType(org.apache.avro.Schema.Field field,
			Map<String, String> options) {
		if (isPrimitiveType(field.schema())) {
			return field.schema().getType();
		}
		if (isSupportedType(field.schema())) {
			switch (field.schema().getType()) {
			case FIXED: {
				options.put(AlterTableRequest.Options.COLUMN_PROPERTIES,
						"string" + (isNullable(field.schema()) ? ",nullable" : ""));
				return org.apache.avro.Schema.Type.BYTES;
			}
			case ENUM: {
				options.put(AlterTableRequest.Options.COLUMN_PROPERTIES,
						"dict" + (isNullable(field.schema()) ? ",nullable" : ""));
				return org.apache.avro.Schema.Type.STRING;
			}
			case BOOLEAN: {
				options.put(AlterTableRequest.Options.COLUMN_PROPERTIES,
						(isNullable(field.schema()) ? ",nullable" : ""));
				return org.apache.avro.Schema.Type.INT;
			}
			case UNION: {
				options.put(AlterTableRequest.Options.COLUMN_PROPERTIES, "nullable");
				for (org.apache.avro.Schema unionSubType : field.schema().getTypes()) {
					if (unionSubType.getType() == org.apache.avro.Schema.Type.NULL) {
						continue;
					} else {
						return unionSubType.getType();
					}
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
	 * 
	 * @param kafkaType  Kafka Schema type provided in incoming data
	 * @param name       column name
	 * @param columns    columns collection
	 * @param properties field properties
	 * @throws Exception
	 */
	public static void addColumn(Schema schema, Schema.Type kafkaType, String prefix, String name, List<Column> columns,
			List<String> properties, String fieldNameDelimiter,
			KineticaSinkConnectorConfig.ARRAY_FLATTENING arrayFlatteningMode, String arrayValueSeparator,
			KineticaFieldMapper mapper) throws Exception {
		Column column;
		String columnName;
		switch (kafkaType) {
		case MAP: {
			Schema mapValueSchema = schema.valueSchema();
			for (Field childField : mapValueSchema.fields()) {
				if ((childField.schema() == null) || (childField.schema().type() == null)) {
					continue;
				}
				addColumn(childField.schema(), childField.schema().type(),
						(prefix.isEmpty() ? "" : prefix + separator) + schema.name(), childField.name(), columns,
						properties, fieldNameDelimiter, arrayFlatteningMode, arrayValueSeparator, mapper);
			}
			break;
		}
		case ARRAY: {
			Schema arrItemSchema = schema.valueSchema();
			if (arrayFlatteningMode.equals(KineticaSinkConnectorConfig.ARRAY_FLATTENING.TAKE_FIRST_VALUE)) {
				addColumn(arrItemSchema.schema(), arrItemSchema.schema().type(),
						(prefix.isEmpty() ? "" : prefix + separator), schema.name(), columns, properties,
						fieldNameDelimiter, arrayFlatteningMode, arrayValueSeparator, mapper);
			} else {
				addColumn(arrItemSchema.schema(), Schema.Type.STRING, prefix, schema.name(), columns, properties,
						fieldNameDelimiter, arrayFlatteningMode, arrayValueSeparator, mapper);
			}
			break;
		}
		case STRUCT: {
			for (Field childField : schema.fields()) {
				if (childField.schema() == null || childField.schema().type() == null) {
					continue;
				}
				addColumn(childField.schema(), childField.schema().type(),
						(prefix.isEmpty() ? "" : prefix + separator) + schema.name(), childField.name(), columns,
						childField.schema().isOptional() ? properties : null, fieldNameDelimiter, arrayFlatteningMode,
						arrayValueSeparator, mapper);
			}
			break;
		}
		case BYTES: {
			columnName = (prefix.isEmpty() ? "" : prefix + separator) + name;
			column = new Column(columnName.replaceAll(separator, fieldNameDelimiter), ByteBuffer.class, properties);
			columns.add(column);
			mapper.getMapped().put(columnName, column);
			mapper.getLookup().put(column.getName(), columnName);
			LOG.debug("\t\t\t" + kafkaType + " [" + columnName + " -> " + column.getName() + "]");
			break;
		}
		case FLOAT64: {
			columnName = (prefix.isEmpty() ? "" : prefix + separator) + name;
			column = new Column(columnName.replaceAll(separator, fieldNameDelimiter), Double.class, properties);
			columns.add(column);
			mapper.getMapped().put(columnName, column);
			mapper.getLookup().put(column.getName(), columnName);
			LOG.debug("\t\t\t" + kafkaType + " [" + columnName + " -> " + column.getName() + "]");
			break;
		}
		case FLOAT32: {
			columnName = (prefix.isEmpty() ? "" : prefix + separator) + name;
			column = new Column(columnName.replaceAll(separator, fieldNameDelimiter), Float.class, properties);
			columns.add(column);
			mapper.getMapped().put(columnName, column);
			mapper.getLookup().put(column.getName(), columnName);
			LOG.debug("\t\t\t" + kafkaType + " [" + columnName + " -> " + column.getName() + "]");
			break;
		}
		case INT32: {
			columnName = (prefix.isEmpty() ? "" : prefix + separator) + name;
			column = new Column(columnName.replaceAll(separator, fieldNameDelimiter), Integer.class, properties);
			columns.add(column);
			mapper.getMapped().put(columnName, column);
			mapper.getLookup().put(column.getName(), columnName);
			LOG.debug("\t\t\t" + kafkaType + " [" + columnName + " -> " + column.getName() + "]");
			break;
		}
		case INT64: {
			columnName = (prefix.isEmpty() ? "" : prefix + separator) + name;
			column = new Column(columnName.replaceAll(separator, fieldNameDelimiter), Long.class, properties);
			columns.add(column);
			mapper.getMapped().put(columnName, column);
			mapper.getLookup().put(column.getName(), columnName);
			LOG.debug("\t\t\t" + kafkaType + " [" + columnName + " -> " + column.getName() + "]");
			break;
		}
		case BOOLEAN: {
			columnName = (prefix.isEmpty() ? "" : prefix + separator) + name;
			column = new Column(columnName.replaceAll(separator, fieldNameDelimiter), Integer.class, properties);
			columns.add(column);
			mapper.getMapped().put(columnName, column);
			mapper.getLookup().put(column.getName(), columnName);
			LOG.debug("\t\t\t" + kafkaType + " [" + columnName + " -> " + column.getName() + "]");
			break;
		}
		case STRING: {
			columnName = (prefix.isEmpty() ? "" : prefix + separator) + name;
			column = new Column(columnName.replaceAll(separator, fieldNameDelimiter), String.class, properties);
			columns.add(column);
			mapper.getMapped().put(columnName, column);
			mapper.getLookup().put(column.getName(), columnName);
			LOG.debug("\t\t\t" + kafkaType + " [" + columnName + " -> " + column.getName() + "]");
			break;
		}
		default: {
			// when the column type is not in the list of provided primitive types,
			// Kafka schema is invalid and Kafka data can't be exported to Kinetica table
			Exception ex = new ConnectException("Unsupported type for field " + name + ".");
			throw ex;
		}
		}
	}

	/**
	 * Checks if the provided schema defines a primitive Type
	 * 
	 * @param schema incoming Kafka schema
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

		case BOOLEAN:
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
	 * 
	 * @param schema incoming Avro schema
	 * @return true when the type is primitive
	 */
	public static boolean isPrimitiveType(org.apache.avro.Schema schema) {
		org.apache.avro.Schema.Type type = schema.getType();
		switch (type) {
		case BOOLEAN:
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
	 * 
	 * @param schema incoming Avro schema
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
			if (schema.getTypes() != null) {
				if (schema.getTypes().size() > 2) {
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
	 * 
	 * @param schema incoming avro schema
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
			if (schema.getTypes() != null && schema.getTypes().size() > 0) {
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
	 * 
	 * @param column Kinetica column type
	 * @return
	 */
	public static String getKineticaColumnType(Column column) {
		if (column.getType() == null)
			return "null";
		switch (column.getType().getSimpleName()) {
		case "Integer":
			return "int";
		case "Double":
		case "Float":
		case "Long":
		case "String":
			return column.getType().getSimpleName().toLowerCase();
		case "ByteBuffer":
			return "byte";
		default:
			return "null";
		}
	}

	public static String flattenArray(String arrayValueSeparator, KineticaSinkConnectorConfig.ARRAY_FLATTENING mode,
			List inList) {

		if (inList == null || inList.size() == 0)
			return null;

		String outValue = null;
		Object item = inList.get(0);

		switch (mode) {
		case CONVERT_TO_STRING: {
			// Invoke flattening on all JSON array values and concatenate with configured
			// arrayValueSeparator
			if (item instanceof String) {
				outValue = String.join(arrayValueSeparator, inList);
			} else if (item instanceof Integer || item instanceof Long || item instanceof Double
					|| item instanceof Float) {
				StringJoiner joiner = new StringJoiner(arrayValueSeparator);
				inList.forEach(o -> joiner.add(o.toString()));
				outValue = joiner.toString();
			} else if (item instanceof Map) {
				// Flatten inner structure to string
				StringJoiner outerJoiner = new StringJoiner(arrayValueSeparator);
				for (Object member : inList) {
					StringJoiner innerJoiner = new StringJoiner(arrayValueSeparator);
					for (Object key : ((Map<Object, Object>) member).keySet()) {
						Object mapValue = ((Map<Object, Object>) member).get(key);
						String mapValueAsString = (mapValue instanceof Integer || mapValue instanceof Long
								|| mapValue instanceof Double || mapValue instanceof Float) ? mapValue.toString()
										: "'" + (String) mapValue + "'";
						innerJoiner.add(String.format("'%s'=%s", key.toString(), mapValueAsString));
					}
					outerJoiner.add("{" + innerJoiner.toString() + "}");
				}
				outValue = outerJoiner.toString();
			} else if (item instanceof Struct) {
				// Flatten inner structure to string
				StringJoiner joiner = new StringJoiner(arrayValueSeparator);
				inList.forEach(o -> joiner.add(o.toString()));
				outValue = joiner.toString().replaceAll("Struct", "");
			} else {
				StringJoiner joiner = new StringJoiner(arrayValueSeparator);
				inList.forEach(o -> joiner.add(o.toString()));
				outValue = joiner.toString();
			}
			break;
		}
		default:
			outValue = null;
			break;
		}
		return outValue;
	}
}
