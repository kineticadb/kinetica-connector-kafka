package com.kinetica.kafka.data.utils;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
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
    
    public static String getRandomAlphaNumericString(int length){
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuilder str = new StringBuilder();
       
        for( int i = 0; i < length; i++ ){
            str.append(chars.charAt(random.nextInt(chars.length())));
        }
        return(str.toString());
    }
    
    public static final DecimalFormat f = new DecimalFormat("0.000");
    
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
    
    /*
     * Helper function. Populates a simple Struct record with random values mimicking 
     * actual topic messages.
     */    
    public static Struct populateStruct(org.apache.kafka.connect.data.Schema schema) {
        
        Struct struct = new Struct(schema);
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            struct.put(field.name(), randomValueByType(field.schema()));
        }
        return struct;
    }

    /*
     * Helper function. Populates a Kafka schema record.
     * Generates random values mimicking actual Ticker trading messages.
     */
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

    /*
     * Helper function. Populates a Kafka schema record with nested Struct and Array types.
     * Generates random values mimicking actual ElasticSearch topic messages.
     */
    public static Struct populateES_GEO1_Record(int num) {        
        
    	int max = random.nextInt(5);
    	
    	Struct geoip = getGeoipStruct();
        
        List<String> certs = new ArrayList<String>();
        for (int i = 0; i < max; i++)
            certs.add(getRandomAlphaNumericString(64).toLowerCase());

    	Struct struct = new Struct(SchemaRegistryUtils.KAFKA_ES_GEO1_NESTED_SCHEMA);
    	struct.put("property1", "some_level");
    	struct.put("property_geo", geoip);
        struct.put("property2", getRandomAlphaNumericString(20)); 
        struct.put("property3", getRandomAlphaNumericString(40)); 
        struct.put("property_ts1", (new Date()).getTime());
        struct.put("property4", getRandomAlphaNumericString(20).toUpperCase()+":s01");
        struct.put("property_ip1", getIP());
        struct.put("n", num);
        struct.put("property5", "www." + getRandomAlphaNumericString(12).toLowerCase() + ".com");
        struct.put("property_ip1", getIP());
        struct.put("property_port1", random.nextInt(10000));
    	struct.put("property6", certs);
        struct.put("property_port2", random.nextInt(10000));
        struct.put("property7", getRandomAlphaNumericString(32).toLowerCase());
        struct.put("property_ts2", (new Date()).getTime());
        
        return struct;
    }
    
    public static Struct populateES_GEO2_Record(int num) {        
    	int max = random.nextInt(5);
    	
    	ArrayList<Struct> geoIPs = new ArrayList<Struct>();
        for (int i = 0; i < max; i++) {
        	geoIPs.add(getGeoipStruct());
        }
        
        Struct struct = new Struct(SchemaRegistryUtils.KAFKA_ES_GEO2_NESTED_SCHEMA);
        struct.put("property1", "some_level");
        struct.put("property_geo", geoIPs);
        struct.put("property_ts1", (new Date()).getTime());
        struct.put("property2", "www." + getRandomAlphaNumericString(12).toLowerCase() + ".com");
        String[] data = new String[max];
    	for (int i=0; i<max; i++) {
    		data[i] = getIP();
    	}
    	struct.put("property3", String.join(",", data));
    	struct.put("property4", getRandomAlphaNumericString(20).toUpperCase()+":sensor01");
    	struct.put("property5", getRandomAlphaNumericString(32).toLowerCase());
    	struct.put("property6", 
            String.format("%d,%d,%d,%d,%d,%d,%d,%d", 
                    random.nextInt(1000), random.nextInt(1000), random.nextInt(1000), random.nextInt(1000),
                    random.nextInt(1000), random.nextInt(1000), random.nextInt(1000), random.nextInt(1000)
        )); 
        struct.put("property_ip1", getIP());
        struct.put("n", num);
        struct.put("property_ip2", getIP());
        struct.put("property_port1", random.nextInt(10000));
        struct.put("property_port2", random.nextInt(10000));
        struct.put("property_ts2", (new Date()).getTime());
        struct.put("property7", random.nextInt(3));

        return struct;
    } 
    
    public static Struct populateCase1Record() {        
		Struct objectB = new Struct(SchemaRegistryUtils.KAFKA_CASE1_SCHEMA_OBJECT_B);
		objectB.put("type", random.nextInt(10));
    	
    	Struct objectA = new Struct(SchemaRegistryUtils.KAFKA_CASE1_SCHEMA_OBJECT_A);
		objectA.put("property_num", random.nextInt(10000));
		objectA.put("objectB", objectB);
		
    	Struct struct = new Struct(SchemaRegistryUtils.KAFKA_CASE1_SCHEMA);
		struct.put("property_ts", RandomGenerator.getFormattedRandomDate());
		struct.put("property_desc", RandomGenerator.getRandomString(15));
	    struct.put("property_message", RandomGenerator.getRandomString(10));
	    struct.put("objectA", objectA);
	    return struct;
    }
    
    public static Struct populateCase2Record() {
    	Struct objectE = new Struct(SchemaRegistryUtils.KAFKA_CASE2_OBJ_E); 
    	objectE.put("hasProperty1", random.nextBoolean());
    	objectE.put("hasProperty2", random.nextBoolean());
    	
    	Struct objectA = new Struct(SchemaRegistryUtils.KAFKA_CASE2_OBJ_A);
    	objectA.put("propertyA1", random.nextBoolean());
    	objectA.put("propertyA2", random.nextInt(10));
    	objectA.put("propertyA3", RandomGenerator.getFormattedRandomDate());
    	objectA.put("propertyA4", getRandomString(10));
    	
    	Struct objectB = new Struct(SchemaRegistryUtils.KAFKA_CASE2_OBJ_B);
    	ArrayList<Struct> adjs = new ArrayList<Struct>();
    	for (int i=0; i<2; i++) {
    		Struct adj = new Struct(SchemaRegistryUtils.KAFKA_CASE2_OBJ_B_COLLECTION_ITEM);
			adj.put("collection_item1", getRandomString(10));
			adj.put("collection_item2", getRandomString(10));
			adj.put("collection_item3", getRandomString(10));
			adj.put("collection_item4", random.nextInt(2));
			adj.put("collection_item5", RandomGenerator.getFormattedRandomDate());
			adj.put("collection_item6", getRandomString(10));
			adj.put("collection_item7", getRandomString(10));
			adj.put("collection_item8", random.nextBoolean());
			adj.put("collection_item9", getRandomString(10));
			adj.put("collection_item10", random.nextInt(2));
    		adjs.add(adj);
    	}
    	objectB.put("objectBcollection", adjs);
    	objectB.put("isObjectB", random.nextBoolean());
    	
    	Struct objectC = new Struct(SchemaRegistryUtils.KAFKA_CASE2_OBJ_C);
    	objectC.put("objectCproperty", random.nextInt(10));
    	
    	Struct subObjectD = new Struct(SchemaRegistryUtils.KAFKA_CASE2_QF);
    	ArrayList<String> subGroupsD = new ArrayList<String>();
    	for (int i=0; i<3; i++) 
    		subGroupsD.add(getRandomString(8));
    	subObjectD.put("subGroupsD", subGroupsD);
    	
    	Struct objectD = new Struct(SchemaRegistryUtils.KAFKA_CASE2_OBJ_D);
    	objectD.put("subObjectD", subObjectD);
    	objectD.put("isA", random.nextBoolean());
    	objectD.put("isB", random.nextBoolean());
    	objectD.put("isC", random.nextBoolean());
    	objectD.put("isD", random.nextBoolean());
    	objectD.put("isE", random.nextBoolean());
    	objectD.put("isF", random.nextBoolean());
    	objectD.put("isG", random.nextBoolean());
    	objectD.put("isH", random.nextBoolean());
    	objectD.put("isI", random.nextBoolean());
    	
    	Struct detail = new Struct(SchemaRegistryUtils.KAFKA_CASE2_OBJ_DETAIL);
    	detail.put("property1", getRandomString(10));
    	detail.put("property2", random.nextBoolean());
    	detail.put("objectA", objectA);
		detail.put("objectB", objectB);
    	detail.put("objectC", objectC);
    	detail.put("objectD", objectD);
    	detail.put("objectE", objectE);
    	detail.put("property3", random.nextInt(10));

    	ArrayList<String> objectF = new ArrayList<String>();
    	for (int i=0; i<2; i++) 
    		objectF.add(getRandomString(8));
    	detail.put("objectF", objectF);
    	
    	detail.put("property4", random.nextBoolean());
    	detail.put("property5", RandomGenerator.getRandomString(5));
    	detail.put("property6", RandomGenerator.getRandomString(5));
    	detail.put("property7", random.nextInt(10));

    	Struct result = new Struct(SchemaRegistryUtils.KAFKA_CASE2_SCHEMA);
    	result.put("top_object_id", new String("" + random.nextInt(1000)));
    	result.put("top_object_details", detail);

    	return result;
    }

    public static HashMap<String, Object> populateNestedTypeRecordWithHashMap(org.apache.kafka.connect.data.Schema schema, int num) {        
    	HashMap<String, Object> result = new HashMap<String, Object>();
        double lat = new Double(f.format(random.nextDouble()*90));
        double lon = new Double(f.format(-random.nextDouble()*90));
		HashMap<String, Object> geoip = new HashMap<String, Object>();

		switch(schema.name()) {
    	case "es_geoip1": {
    		result.put("property1", "some_level");
    		
    		geoip.put("cc2", "US");
    		geoip.put("lon", lon);
            geoip.put("location", String.format("%.3f,%.3f\n", lat, lon));
            geoip.put("lat", lat);
            result.put("property_geo", geoip);
            
            result.put("property2", getRandomAlphaNumericString(20));
            result.put("property3", getRandomAlphaNumericString(40));
            result.put("property_ts1", (new Date()).getTime());
            result.put("property4", getRandomAlphaNumericString(20).toUpperCase()+":s01");
            result.put("property_ip1", getIP());
            result.put("n", num);
            result.put("property5", "www." + getRandomAlphaNumericString(12).toLowerCase() + ".com");
            result.put("property_ip2", getIP());
            result.put("property_port1", random.nextInt(10000));
            
            List<String> certs = new ArrayList<String>();
            for (int i=0; i<5; i++) certs.add(getRandomAlphaNumericString(64).toLowerCase());

            result.put("property6", certs);
            result.put("property_port2", random.nextInt(10000));
            result.put("property7", getRandomAlphaNumericString(32).toLowerCase());
            result.put("property_ts2", (new Date()).getTime());
    		break;
    	}	
    	case "es_geoip2": {
    		result.put("property1", "some_level");
    		
    		geoip.put("cc2", "US");
    		geoip.put("lon", lon);
            geoip.put("location", String.format("%.3f,%.3f\n", lat, lon));
            geoip.put("lat", lat);
            result.put("property_geo", geoip);
            
            result.put("property_ts1", (new Date()).getTime());
            result.put("property2", "data-live." + getRandomAlphaNumericString(12).toLowerCase() + ".com");
            
            String[] data = new String[5];
        	for (int i=0; i<5; i++) {
        		data[i] = getIP();
        	}
        	result.put("property3", String.join(",", data));
            
        	result.put("property4", getRandomAlphaNumericString(20).toUpperCase()+":sensor01");
            result.put("property5", getRandomAlphaNumericString(32).toLowerCase());
            result.put("property6", String.format("%d,%d,%d,%d,%d,%d,%d,%d", 
                    random.nextInt(1000), random.nextInt(1000), random.nextInt(1000), random.nextInt(1000),
                    random.nextInt(1000), random.nextInt(1000), random.nextInt(1000), random.nextInt(1000)));

            result.put("property_ip1", getIP());
            result.put("n", num);
            result.put("property_ip2", getIP());
            result.put("property_port1", random.nextInt(10000));
            
            result.put("property_port2", random.nextInt(10000));
            result.put("property_ts2", (new Date()).getTime());
            result.put("property7", random.nextInt(10000));
    		break;
    	}	
    	default:
    		break;
    	}
    	return result;
    }
    
    public static Struct getGeoipStruct() {
    	Struct geoipStruct = new Struct(SchemaRegistryUtils.KAFKA_GEOIP_SCHEMA);
        double lat = new Double(f.format(random.nextDouble()*90));
        double lon = new Double(f.format(-random.nextDouble()*90));
        geoipStruct.put("cc2", "US");
        geoipStruct.put("lon", lon);
        geoipStruct.put("location", String.format("%.3f,%.3f\n", lat, lon));
        geoipStruct.put("lat", lat); 
        
        return geoipStruct;
    }
    
    public static String getIP() {
    	return String.format("%d.%d.%d.%d", 
                random.nextInt(256), random.nextInt(256), random.nextInt(256), random.nextInt(256));
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
