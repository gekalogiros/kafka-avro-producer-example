package com.gekalogiros.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProducerRecordFactoryTest {

    private static final String MESSAGE_PREFIX = "This is an example body create from ";
    private static final String TOPIC = "kafka-topic";
    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{\"namespace\":\"com.gekalogiros.kafka\",\"type\":\"record\",\"name\":\"KafkaMessage\",\"fields\":[{\"name\":\"body\",\"type\":\"string\"}]}");

    private ProducerRecordFactory underTest;

    @BeforeEach
    void setUp() {
        this.underTest = new ProducerRecordFactory(TOPIC, SCHEMA);
    }

    @Test
    void fromData() {

        final ProducerRecord<String, GenericRecord> actual = underTest.fromData();

        final ProducerRecord<String, GenericRecord> expected = expected(actual.key(), "map");

        assertEquals(actual, expected);
    }

    @Test
    void fromObject() {

        final ProducerRecord<String, GenericRecord> actual = underTest.fromObject();

        final ProducerRecord<String, GenericRecord> expected = expected(actual.key(), KafkaMessage.newBuilder()
                .setBody(MESSAGE_PREFIX + "object")
                .build());

        assertEquals(actual, expected);
    }

    @Test
    void fromJson() {

        final ProducerRecord<String, GenericRecord> actual = underTest.fromJson();

        final ProducerRecord<String, GenericRecord> expected = expected(actual.key(), "json");

        assertEquals(actual, expected);
    }

    private ProducerRecord<String, GenericRecord> expected(final String key, final String type) {

        final GenericRecord record = new GenericRecordBuilder(SCHEMA)
                .set("body", MESSAGE_PREFIX + type)
                .build();

        return expected(key, record);
    }

    private ProducerRecord<String, GenericRecord> expected(final String key, final GenericRecord genericRecord) {
        return new ProducerRecord<>(TOPIC, key, genericRecord);
    }
}