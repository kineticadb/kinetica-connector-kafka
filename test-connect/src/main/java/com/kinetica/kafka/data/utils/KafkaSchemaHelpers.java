package com.kinetica.kafka.data.utils;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class KafkaSchemaHelpers {
    public static final String[] FRUITS = new String[] {"Apple", "Orange", "Banana"};
    public static Random random = new Random();
    
    public static String getRandomString(int length){
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz ";
        StringBuilder str = new StringBuilder();
       
        for( int i = 0; i < length; i++ ){
            str.append(chars.charAt(random.nextInt(chars.length())));
        }
        return(str.toString());
    }
    
    // Helper function, generating pseudo-random values for 4-character Stock abbreviation
    public static String getTicker(int number) {
    	return "ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ".substring(number%26, (number%26) + 4);
    }
    
    private static final Schema appleSchema = SchemaBuilder.struct()
            .name("com.kinetica.kafka.data.Apple")
            .field("id", Schema.INT32_SCHEMA)
            .field("sort", Schema.STRING_SCHEMA)
            .field("size", Schema.INT32_SCHEMA)
            .field("weight", Schema.FLOAT32_SCHEMA)
            .build();
    
    private static final Schema orangeSchema = SchemaBuilder.struct()
            .name("com.kinetica.kafka.data.Orange")
            .field("id", Schema.INT32_SCHEMA)
            .field("sort", Schema.STRING_SCHEMA)
            .field("diameter", Schema.INT32_SCHEMA)
            .field("weight", Schema.FLOAT32_SCHEMA)
            .build();

    private static final Schema bananaSchema = SchemaBuilder.struct()
            .name("com.kinetica.kafka.data.Banana")
            .field("id", Schema.INT32_SCHEMA)
            .field("sort", Schema.STRING_SCHEMA)
            .field("length", Schema.FLOAT32_SCHEMA)
            .field("weight", Schema.FLOAT32_SCHEMA)
            .build();

    public static Schema getKafkaSchema(String className) {
        switch(className) {
        case "Apple" : return appleSchema;
        case "Orange" : return orangeSchema;
        case "Banana" : return bananaSchema;
        default: return null;
        }
    }
    
    public static Struct populateStruct(org.apache.kafka.connect.data.Schema schema) {
        
        Struct struct = new Struct(schema);
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            struct.put(field.name(), randomValueByType(field.schema()));
        }
        return struct;
    }
    
    public static Struct populateTicker(org.apache.kafka.connect.data.Schema schema, int num) {
        
        Struct struct = new Struct(schema);
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
        	// Helper function, generating pseudo-random values for Ticker record
            switch (field.name()) {
	            case "symbol" : 
	            	struct.put(field.name(), getTicker(num)); break;
	            case "securityType" : 
	            	struct.put(field.name(), "commonstock"); break;
	            case "bidPrice" : 
	            case "askPrice" : 
	            case "lastSalePrice" : 
	            	struct.put(field.name(), 50*(1+random.nextFloat())); break;
	            case "bidSize" : 
	            case "askSize" : 
	            case "lastSaleSize" : 
	            	struct.put(field.name(), random.nextInt(1000)); break;
	            case "volume" : 
	            	struct.put(field.name(), random.nextInt(10000)); break;
	            case "lastUpdated" : 
	            case "lastSaleTime" : 
	            	struct.put(field.name(), (new Date()).getTime()); break;
	            case "marketPercent" : 
	            	struct.put(field.name(), random.nextFloat()); break;
	            default : 
	            	struct.put(field.name(), null); break;
	            }        	
        	}
        return struct;
    }
    
    public static  Object randomValueByType (org.apache.kafka.connect.data.Schema schema) {
        org.apache.kafka.connect.data.Schema.Type type = schema.type();
        switch (type) {
        case STRING:  return getRandomString(24);
        case BYTES:   return ByteBuffer.wrap(getRandomString(48).getBytes());
        case INT8: case INT16: case INT32:     
                      return random.nextInt(1000);
        case INT64:    return random.nextLong();
        case FLOAT32:   return random.nextFloat();
        case FLOAT64:  return random.nextDouble();
            
        default:      return null;
        }
    }

    public static Map<String, Object> populateConsumerRecord(String className) {
        org.apache.kafka.connect.data.Schema schema = getKafkaSchema(className);
        Map<String, Object> record = new HashMap<String, Object>();
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            record.put(field.name(), randomValueByType(field.schema()));
        }
        return record;
    }

    public static Map<String, Object> populateConsumerRecord(String className, float percent) {
        org.apache.kafka.connect.data.Schema schema = getKafkaSchema(className);
        Map<String, Object> record = new HashMap<String, Object>();
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
        	// add null values to 'percent' number of nullable fields    
        	record.put(field.name(), (!"id".equalsIgnoreCase(field.name()) && (random.nextFloat()<percent) ) ? null : randomValueByType(field.schema()));
        }
        return record;
    }

}
