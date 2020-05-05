package com.kinetica.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PatternOptionBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kinetica.kafka.data.Apple;
import com.kinetica.kafka.data.Banana;
import com.kinetica.kafka.data.Orange;
import com.kinetica.kafka.data.utils.KafkaSchemaHelpers;
import com.kinetica.kafka.data.utils.RandomGenerator;
import com.kinetica.kafka.data.utils.SchemaRegistryUtils;

public class JsonDataPump {
    
    // command line params
    private static String bootstrapUrl = null;
    private static String schemaRegistry = null;
    private static String topic = "fruits";
    protected static int batchSize = 10;
    protected static int count = 10;
    private final static String NULL_SUFFIX = "_with_nulls";
    
    static Schema appleSchema = new Schema.Parser().parse(Apple.getSchema());
    static Schema bananaSchema = new Schema.Parser().parse(Banana.getSchema());
    static Schema orangeSchema = new Schema.Parser().parse(Orange.getSchema());


    /**
     * Helper function  - populates Kafka topic with 3 different types of plain JSON messages
     * No Schema Registry 
     * @param topic
     * @throws Exception
     */

    public static void ProduceJSONMessages(String topic) throws Exception
    {
        // Start KAFKA publishing
        Properties props = new Properties();
        props.put("bootstrap.servers", JsonDataPump.bootstrapUrl);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");
        
        ObjectMapper objectMapper = new ObjectMapper();
        
        KafkaProducer<String, JsonNode> messageProducer = new KafkaProducer<String, JsonNode>(props);
        ProducerRecord<String, JsonNode> producerRecord;
        String key;
        JsonNode jsonNode;
        for (int i=0; i<JsonDataPump.count; i++) {
            for (int j=0; j<JsonDataPump.batchSize; j++) {
                // send a batch of Apples
                key = "Apple";
                jsonNode = objectMapper.valueToTree(Apple.generateApple(i*JsonDataPump.batchSize + j));
                producerRecord = new ProducerRecord<String, JsonNode>(topic, key, jsonNode);
                messageProducer.send(producerRecord);
                System.out.println(key + " " + producerRecord);
                
                // send a batch of Orabges
                key = "Orange";
                jsonNode = objectMapper.valueToTree(Orange.generateOrange(i*JsonDataPump.batchSize + j));
                producerRecord = new ProducerRecord<String, JsonNode>(topic, key, jsonNode);
                messageProducer.send(producerRecord);
                System.out.println(key + " " + producerRecord);
                
                // send a batch of Bananas
                key = "Banana";
                jsonNode = objectMapper.valueToTree(Banana.generateBanana(i*JsonDataPump.batchSize + j));
                producerRecord = new ProducerRecord<String, JsonNode>(topic, key, jsonNode);
                messageProducer.send(producerRecord);
                System.out.println(key + " " + producerRecord);            
            }
        }
        messageProducer.close();
    }  
    
    /**
     * Helper function  - populates Kafka topic with 3 different types of Avro serialized messages 
     * Schema Registry must be running on Kafka side. Address should be provided in command-line args 
     * @param topic
     * @throws Exception
     */
    public static void ProduceAvroMessages(String topic) throws Exception
    {
        // Start KAFKA publishing
        Properties props = new Properties();
        props.put("bootstrap.servers", JsonDataPump.bootstrapUrl);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", JsonDataPump.schemaRegistry);
        
        KafkaProducer<Object, Object> messageProducer = new KafkaProducer<Object, Object>(props);
        ProducerRecord<Object, Object> producerRecord;
        String key;
        GenericRecord obj;
        for (int i=0; i<JsonDataPump.count; i++) {
            for (int j=0; j<JsonDataPump.batchSize; j++) {
                
                // send a batch of Apples
                key = "Apple";
                obj = RandomGenerator.populateGenericRecord(appleSchema);
                producerRecord = new ProducerRecord<Object, Object>(topic, key, obj);
                messageProducer.send(producerRecord);
                System.out.println(key + " " + producerRecord);
                if (i==0 && j==0) {
                    // for a newly created topic, set compatibility level to accept all types
                    SchemaRegistryUtils.setSchemaCompatibility(JsonDataPump.schemaRegistry, topic, SchemaRegistryUtils.CompatibilityLevel.NONE);
                }
                
                //send a batch of Oranges
                key = "Orange";
                obj = RandomGenerator.populateGenericRecord(orangeSchema);
                producerRecord = new ProducerRecord<Object, Object>(topic, key, obj);
                messageProducer.send(producerRecord);
                System.out.println(key + " " + producerRecord);
                
                //send a batch of Bananas
                key = "Banana";
                obj = RandomGenerator.populateGenericRecord(bananaSchema);
                producerRecord = new ProducerRecord<Object, Object>(topic, key, obj);
                messageProducer.send(producerRecord);
                System.out.println(key + " " + producerRecord);
                
            }
        }
        messageProducer.close();
    }  
    public static void ProduceAvroMessagesWithSchema(org.apache.avro.Schema schema, String topic) throws Exception
    {
        // Start KAFKA publishing
        Properties props = new Properties();
        props.put("bootstrap.servers", JsonDataPump.bootstrapUrl);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", JsonDataPump.schemaRegistry);
        
        KafkaProducer<Object, Object> messageProducer = new KafkaProducer<Object, Object>(props);
        ProducerRecord<Object, Object> producerRecord;
        String key;
        GenericRecord obj;
        for (int i=0; i<JsonDataPump.count; i++) {
            for (int j=0; j<JsonDataPump.batchSize; j++) {
                
                // send a batch of Apples
                key = "Apple";
                obj = RandomGenerator.populateGenericRecord(appleSchema);
                producerRecord = new ProducerRecord<Object, Object>(topic, key, obj);
                messageProducer.send(producerRecord);
                System.out.println(key + " " + producerRecord);
                if (i==0 && j==0) {
                    // for a newly created topic, set compatibility level to accept all types
                    SchemaRegistryUtils.setSchemaCompatibility(JsonDataPump.schemaRegistry, topic, SchemaRegistryUtils.CompatibilityLevel.NONE);
                }
                
                //send a batch of Oranges
                key = "Orange";
                obj = RandomGenerator.populateGenericRecord(orangeSchema);
                producerRecord = new ProducerRecord<Object, Object>(topic, key, obj);
                messageProducer.send(producerRecord);
                System.out.println(key + " " + producerRecord);
                
                //send a batch of Bananas
                key = "Banana";
                obj = RandomGenerator.populateGenericRecord(bananaSchema);
                producerRecord = new ProducerRecord<Object, Object>(topic, key, obj);
                messageProducer.send(producerRecord);
                System.out.println(key + " " + producerRecord);
                
            }
        }
        messageProducer.close();
    }  

    
    public static void ProduceMessages(String bootstrapUrl, int size, int count, String schemaRegistry, String topic) throws Exception
    {
        JsonDataPump.bootstrapUrl = bootstrapUrl;
        JsonDataPump.batchSize = size;
        JsonDataPump.count = count;
        JsonDataPump.schemaRegistry = schemaRegistry;
        JsonDataPump.topic = topic;
        
        if (JsonDataPump.schemaRegistry!=null && !JsonDataPump.schemaRegistry.isEmpty()) {
            ProduceAvroMessages(topic);
        } else {
            ProduceJSONMessages(topic);
        }
    }
    
    public static List<SinkRecord> getSinkRecordJSONMessages(String topic) {
        // Start KAFKA consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", JsonDataPump.bootstrapUrl);
        props.put("group.id", "KafkaExampleConsumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.connect.json.JsonDeserializer");
        
        KafkaConsumer<String, JsonNode> messageConsumer = new KafkaConsumer<String, JsonNode>(props);
        messageConsumer.subscribe(Collections.singletonList(topic));
        
        ConsumerRecords<String, JsonNode> records = messageConsumer.poll(1000);
        messageConsumer.close();
        
        List<SinkRecord> sinkRecords = new ArrayList<SinkRecord>();
        
        ObjectMapper objectMapper = new ObjectMapper();
        
        for (ConsumerRecord<String, JsonNode> entry : records.records(topic)) {
            
            Map<String, Object> result = objectMapper.convertValue(entry.value(), HashMap.class);
            sinkRecords.add(new SinkRecord(entry.topic(), entry.partition(), null, entry.key(), null, result, entry.timestamp()));
            
        }
        return sinkRecords;
    }
    
    public static List<SinkRecord> mockSinkRecordJSONMessages(String topic) {
        List<SinkRecord> sinkRecords = new ArrayList<SinkRecord>();
        
        Map<String, Object> data;
        
        for (int i=0; i<JsonDataPump.count; i++) {
            for (int j=0; j<JsonDataPump.batchSize; j++){
                for (String key : KafkaSchemaHelpers.FRUITS) {
                    data = KafkaSchemaHelpers.populateConsumerRecord(key);
                    sinkRecords.add(new SinkRecord(topic, 0, null, key, null, data, new Date().getTime()));
                }
            }
        }
        return sinkRecords;
    }    

    public static List<SinkRecord> mockSinkRecordJSONMessagesWithNulls(String topic) {
        List<SinkRecord> sinkRecords = new ArrayList<SinkRecord>();

        Map<String, Object> data;
        float percent = 0.15f;
        
        for (int i=0; i<JsonDataPump.count; i++) {
            for (int j=0; j<JsonDataPump.batchSize; j++){
                for (String key : KafkaSchemaHelpers.FRUITS) {
                    if(i != 0 && j != 0) {
                    	data = KafkaSchemaHelpers.populateConsumerRecord(key, percent);
                    } else {
                    	data = KafkaSchemaHelpers.populateConsumerRecord(key);
                    }
                    sinkRecords.add(new SinkRecord(topic, 0, null, key + NULL_SUFFIX, null, data, new Date().getTime()));
                }
            }
        }
        return sinkRecords;
    }   
    
    public static List<SinkRecord> mockSinkRecordAvroMessages (String topic) {
        List<SinkRecord> sinkRecords = new ArrayList<SinkRecord>();
        SinkRecord record;
        Struct struct; 
        
        for (int i=0; i<JsonDataPump.count; i++) {
            for (int j=0; j<JsonDataPump.batchSize; j++) {
                for (String key : KafkaSchemaHelpers.FRUITS) {
                    struct = KafkaSchemaHelpers.populateStruct(KafkaSchemaHelpers.getKafkaSchema(key));
                    
                    record = new SinkRecord(topic, 0, org.apache.kafka.connect.data.Schema.STRING_SCHEMA, key, 
                            KafkaSchemaHelpers.getKafkaSchema(key), struct, 
                            (JsonDataPump.batchSize*i+j), (new Date()).getTime(), TimestampType.CREATE_TIME);
                    sinkRecords.add(record);
                }
            }
        }
        
        return sinkRecords;
    }

    
    /**
     * Primitive arg parser
     * @param args
     * @throws Exception
     */
    private static void parseArgs(String[] args) throws Exception {
        Options options = new Options();
        Option option;

        option = Option.builder("h")
                .longOpt("help")
                .desc("Show Usage")
                .build();
        options.addOption(option);

        option = Option.builder("n")
                .longOpt("batch-size")
                .desc("Number of records in a batch.")
                .argName("count")
                .type(PatternOptionBuilder.NUMBER_VALUE)
                .hasArg()
                .build();
        options.addOption(option);

        option = Option.builder("c")
                .longOpt("total-batches")
                .desc("Number of batches to insert.")
                .argName("count")
                .hasArg(true)
                .type(PatternOptionBuilder.NUMBER_VALUE)
                .build();
        options.addOption(option);
        
        option = Option.builder("s")
                .longOpt("schema-registry")
                .desc("Schema registry location")
                .argName("url:port")
                .hasArg(true)
                .build();
        options.addOption(option);
        
        option = Option.builder("t")
                .longOpt("topic")
                .desc("Topic name")
                .argName("name")
                .hasArg(true)
                .build();
        options.addOption(option);
        // parse the command line arguments
        CommandLineParser parser = new DefaultParser();
        CommandLine line = parser.parse( options, args );

        if(line.hasOption('h')) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "JsonDataPump [options] URL", options );
            System.exit(1);
        }

        if(line.hasOption('n')) {
            Long intObj = (Long)line.getOptionObject('n');
            System.out.println(String.format("Batch size: %d", intObj));
            JsonDataPump.batchSize = intObj.intValue();
        }

        if(line.hasOption('c')) {
            Long intObj = (Long)line.getOptionObject('c');
            System.out.println(String.format("Total batches: %d", intObj));
            JsonDataPump.count = intObj.intValue();
        }

        if(line.hasOption('s')) {
            String url = (String)line.getOptionObject('s');
            System.out.println(String.format("Schema Registry: %s", url));
            JsonDataPump.schemaRegistry = url;
        }

        if(line.hasOption('t')) {
            String topicName = (String)line.getOptionObject('t');
            System.out.println(String.format("Topic Name: %s", topicName));
            JsonDataPump.topic = topicName;
        }
        // get URL
        if(line.getArgs().length == 0) {
            throw new Exception("Missing bootstrap URL");
        }
        JsonDataPump.bootstrapUrl = line.getArgs()[0];
        System.out.println(String.format("URL: %s", JsonDataPump.bootstrapUrl));

    }
    /**
     * Integration test helper class:
     * Data pump creating Kafka topic and autogenerating messages for it
     * If the Schema Registry URL is provided, serialized avro messages are produced,
     * otherwise schemaless JSON messages produced.
     * During integration testing a properly configured KineticaSinkConnector is supposed to consume all messages 
     * into 3 different tables named after Object class primitive name.
     *  
     * @param args     a list of command line arguments followed by Kafka Bootstrap URL (localhost:9092)
     */

    public static void main(String[] args) throws Exception{
        try {
            parseArgs(args);
        }
        catch(Exception ex) {
            System.out.println("PARAM ERROR: " + ex.getMessage());
            System.exit(1);
        }
    
        if (JsonDataPump.schemaRegistry!=null && !JsonDataPump.schemaRegistry.isEmpty()) {
            System.out.println("Producing Avro messages with schema registry");
            ProduceAvroMessages("avro"+JsonDataPump.topic);
        } else {
            System.out.println("Producing schemaless JSON messages");
            ProduceJSONMessages(JsonDataPump.topic);
        }
    }
    
}
