package com.gekalogiros.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.UUID;

class ProducerRecordFactory {

    private static final String BODY_TEMPLATE = "This is an example body create from %s";

    private final String topic;
    private final Schema schema;

    ProducerRecordFactory(String topic, Schema schema) {
        this.topic = topic;
        this.schema = schema;
    }

    ProducerRecord<String, GenericRecord> fromData() {

        final GenericRecord record = new GenericRecordBuilder(schema)
                .set("body", String.format(BODY_TEMPLATE, "map"))
                .build();

        return new ProducerRecord<>(topic, UUID.randomUUID().toString(), record);
    }

    ProducerRecord<String, GenericRecord> fromObject() {

        final KafkaMessage kafkaMessage = new KafkaMessage(String.format(BODY_TEMPLATE, "object"));

        return new ProducerRecord<>(topic, UUID.randomUUID().toString(), kafkaMessage);
    }

    ProducerRecord<String, GenericRecord> fromJson() {

        final String body = String.format(BODY_TEMPLATE, "json");

        final String json = "{\"body\":\"" + body + "\"}";

        try {

            final DecoderFactory decoderFactory = new DecoderFactory();

            final Decoder decoder = decoderFactory.jsonDecoder(schema, json);

            final DatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);

            final GenericRecord record = reader.read(null, decoder);

            return new ProducerRecord<>(topic, UUID.randomUUID().toString(), record);

        } catch (IOException e) {

            throw new RuntimeException("Failed to convert json to avro");
        }
    }

}
