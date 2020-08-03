package com.kinetica.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
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
import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kinetica.kafka.data.Apple;
import com.kinetica.kafka.data.utils.KafkaSchemaHelpers;

public class StringDataPump {
    // command line params
    private static String bootstrapUrl = null;
    private static String topic = "Apple";
    private static int batchSize = 10;
    private static int count = 10;

    static Schema appleSchema = new Schema.Parser().parse(Apple.getSchema());

    /**
     * This DataPumnp publishes JSON no-schema messages to the provided kafka topic, record key is null.
     * Topic name and Kafka bootstrap server URL (such as http://localhost:9092) should be provided.
     *
     * To consume these JSON messages, configure connect-standalone.properties:
     * key.converter=org.apache.kafka.connect.storage.StringConverter
     * value.converter=org.apache.kafka.connect.json.JsonConverter
     * key.converter.schemas.enable=false
     * value.converter.schemas.enable=false
     *
     * and quickstart-kinetica-sink.properties:
     * topics = <your_topic_name>
     * kinetica.tables.create_table = true
     *
     * @param topic       Kafka topic name to be created
     * @param key         Kafka message key (null or tablename)
     * @throws Exception
     */
    public static void ProduceJSONMessages(String topic, String key) throws Exception
    {
        // Start KAFKA publishing
        Properties props = new Properties();
        props.put("bootstrap.servers", StringDataPump.bootstrapUrl);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");

        ObjectMapper objectMapper = new ObjectMapper();

        KafkaProducer<String, JsonNode> messageProducer = new KafkaProducer<String, JsonNode>(props);
        ProducerRecord<String, JsonNode> producerRecord;
        JsonNode jsonNode;
        for (int i=0; i<StringDataPump.count; i++) {
            System.out.println("Batch " + (i+1) );
            for (int j = 0; j < StringDataPump.batchSize; j++ ) {
                jsonNode = objectMapper.valueToTree(Apple.generateApple(i*StringDataPump.batchSize + j));
                System.out.println(jsonNode);
                producerRecord = new ProducerRecord<String, JsonNode>(topic, key, jsonNode);

                messageProducer.send(producerRecord);
                System.out.println(producerRecord);

            }
        }
        messageProducer.close();
    }

    public static void ProduceJSONMessages(String bootstrap, int size, int count, String topic, String key) throws Exception
    {
        StringDataPump.bootstrapUrl = bootstrap;
        StringDataPump.batchSize = size;
        StringDataPump.count = count;
        ProduceJSONMessages(topic, key);
    }

    public static List<SinkRecord> getSinkRecordJSONMessages(String topic) throws Exception
    {
        // Start KAFKA consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", StringDataPump.bootstrapUrl);
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

    public static List<SinkRecord> mockSinkRecordJSONMessages(String topic, String key) {
        List<SinkRecord> sinkRecords = new ArrayList<SinkRecord>();

        Map<String, Object> data;

        for (int i=0; i<JsonDataPump.count; i++) {
            for (int j=0; j<JsonDataPump.batchSize; j++){
                data = KafkaSchemaHelpers.populateConsumerRecord("Apple");
                sinkRecords.add(new SinkRecord(topic, 0, null, key, null, data, new Date().getTime()));
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
            formatter.printHelp( "StringDataPump [options] URL", options );
            System.exit(1);
        }

        if(line.hasOption('n')) {
            Long intObj = (Long)line.getOptionObject('n');
            System.out.println(String.format("Batch size: %d", intObj));
            StringDataPump.batchSize = intObj.intValue();
        }

        if(line.hasOption('c')) {
            Long intObj = (Long)line.getOptionObject('c');
            System.out.println(String.format("Total batches: %d", intObj));
            StringDataPump.count = intObj.intValue();
        }

        if(line.hasOption('t')) {
            String topicName = (String)line.getOptionObject('t');
            System.out.println(String.format("Topic Name: %s", topicName));
            StringDataPump.topic = topicName;
        }
        // get URL
        if(line.getArgs().length == 0) {
            throw new Exception("Missing bootstrap URL (i.e. http://localhost:9092)");
        }
        StringDataPump.bootstrapUrl = line.getArgs()[0];
        System.out.println(String.format("URL: %s", StringDataPump.bootstrapUrl));

    }

    /**
     * Integration test helper class:
     * Data pump creating Kafka topic and autogenerating messages for it
     * This topic has schemaless JSON (not OGG standard) messages generated, null key.
     * It was created to match default command-line Kafka message producer from Kafka documentation
     * During integration testing a properly configured KineticaSinkConnector is supposed to consume all messages into single table.
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

        System.out.println("Producing schemaless JSON messages");
        ProduceJSONMessages(StringDataPump.topic, null);
    }


}
