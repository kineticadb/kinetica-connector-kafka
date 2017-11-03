package com.kinetica.tests;

import com.google.common.collect.Lists;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaJunitRule;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.ClassRule;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class TestKafkaBroker {

    private static final String TOPIC = "topicY";

    @ClassRule
    public static KafkaJunitRule kafkaRule = new KafkaJunitRule(EphemeralKafkaBroker.create());

    @Test
    public void testKafkaServerIsUp() {
        try (KafkaProducer<String, String> producer = kafkaRule.helper().createStringProducer()) {
            producer.send(new ProducerRecord<>(TOPIC, "keyA", "valueA"));
        }

        try (KafkaConsumer<String, String> consumer = kafkaRule.helper().createStringConsumer()) {
            consumer.subscribe(Lists.newArrayList(TOPIC));
            ConsumerRecords<String, String> records = consumer.poll(500);
            assertThat(records, is(notNullValue()));
            assertThat(records.isEmpty(), is(false));

            ConsumerRecord<String, String> msg = records.iterator().next();
            assertThat(msg, is(notNullValue()));
            assertThat(msg.key(), is(equalTo("keyA")));
            assertThat(msg.value(), is(equalTo("valueA")));
        }
    }
}
