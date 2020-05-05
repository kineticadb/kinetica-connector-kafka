package com.kinetica.kafka.data.utils;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.SerializationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class RandomProducers {
    public static void main(String[] args) {
    	
    	String topic = "testNestedTypeTopic_Jackson";        
        
    	// create instance for properties to access producer configs   
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        //props.put("value.serializer", "com.fasterxml.jackson.databind.JsonSerializer");
//        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
//        props.put("schema.registry.url", "http://localhost:8081");
//        Producer<String, String> producer = new KafkaProducer<String, String>(props);
              
         
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        		org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        		org.apache.kafka.connect.json.JsonSerializer.class);
       // props.put("schema.registry.url", "http://localhost:8081");
        
        KafkaProducer producer = new KafkaProducer(props);

        org.apache.avro.Schema schema = SchemaRegistryUtils.NESTED_SCHEMA;
    	org.apache.avro.Schema innerSchema = new org.apache.avro.Schema.Parser().parse(
    		"{\"name\":\"property_geo\",\"type\":\"record\",\"fields\":["  + 
       		"{\"name\":\"countrycode2\",\"type\":[\"null\",\"string\"]}," + 
       		"{\"name\":\"lon\",\"type\":[\"null\",\"double\"]}," + 
       		"{\"name\":\"location\",\"type\":[\"null\",\"string\"]}," + 
       		"{\"name\":\"lat\",\"type\":[\"null\",\"double\"]}" + 
    	    "]}");
        
        int numRecords = 10;
        
        DecimalFormat f = new DecimalFormat("0.000");
        Random random = new Random();
        
        try {
        	for (int i=0; i<numRecords; i++) {
                String key = "key_"+i;
                GenericRecord avroRecord = new GenericData.Record(schema);
                GenericRecord innerRecord = new GenericData.Record(innerSchema);
                avroRecord.put("property1", "some_level"); 
            	
                double lat = new Double(KafkaSchemaHelpers.f.format(random.nextDouble()*90));
                double lon = new Double(KafkaSchemaHelpers.f.format(-random.nextDouble()*90));
                innerRecord.put("cc2", "US");
                innerRecord.put("lon", lon);
                innerRecord.put("location", String.format("%.3f,%.3f\n", lat, lon));
                innerRecord.put("lat", lat);
                
                avroRecord.put("property_geo", innerRecord);  

            	avroRecord.put("property2", KafkaSchemaHelpers.getRandomAlphaNumericString(20)); 
            	avroRecord.put("property3", KafkaSchemaHelpers.getRandomAlphaNumericString(40)); 
            	avroRecord.put("property_ts1", (new Date()).getTime()); 
            	avroRecord.put("property4", KafkaSchemaHelpers.getRandomAlphaNumericString(20).toUpperCase()+":s01"); 
            	avroRecord.put("property_ip1", 
                        String.format("%d.%d.%d.%d", 
                                random.nextInt(256), random.nextInt(256), random.nextInt(256), random.nextInt(256)
                    )); 
            	avroRecord.put("n", i); 
            	avroRecord.put("property5", "www." + KafkaSchemaHelpers.getRandomAlphaNumericString(12).toLowerCase() + ".com"); 
            	avroRecord.put("property_ip2", 
                        String.format("%d.%d.%d.%d", 
                                random.nextInt(256), random.nextInt(256), random.nextInt(256), random.nextInt(256)
                    )); 
            	avroRecord.put("property_port1", random.nextInt(10000)); 
            	
            	int max = random.nextInt(5);
                List<String> certs = new ArrayList<String>();
                for (int j=0; i<max; i++)
                    certs.add(KafkaSchemaHelpers.getRandomAlphaNumericString(64).toLowerCase());
                avroRecord.put("property6", certs); 
            	avroRecord.put("property_port2", random.nextInt(10000)); 
            	avroRecord.put("property7", KafkaSchemaHelpers.getRandomAlphaNumericString(32).toLowerCase()); 
            	avroRecord.put("property_ts2", (new Date()).getTime()); 

                ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, key, avroRecord);

        		producer.send(record);
        	}
        } catch(SerializationException e) {
          // may need to do something with it
        }
        // When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
        // then close the producer to free its resources.
        finally {
          producer.flush();
          producer.close();
        }
/*        for(int i = 0; i < 10; i++)
           producer.send(new ProducerRecord<String, String>(topic, 
              Integer.toString(i), Integer.toString(i)));
                 System.out.println("Message sent successfully");
                 producer.close();*/


    
/*    	Integer kafkaPartition = 0;
    	Schema keySchema = Schema.STRING_SCHEMA;
    	Schema valueSchema = SchemaRegistryUtils.KAFKA_NESTED_SCHEMA;
    	Map<String, ?> sourcePartition = new HashMap<>();
    	Map<String, ?> sourceOffset = new HashMap<>();
    	List<SourceRecord> records = new ArrayList<SourceRecord>();
    	
    	for (int i = 0; i<numRecords; i++) { 
    		long timestamp = System.currentTimeMillis();
    		Object key = new String("key_" + timestamp);
    		Struct value = KafkaSchemaHelpers.populateNestedTypeRecord(valueSchema, i);
    		SourceRecord record = new SourceRecord(sourcePartition, sourceOffset, topic, kafkaPartition, keySchema, key, valueSchema, value, timestamp);
    		
    		
    	}*/
    }

}
