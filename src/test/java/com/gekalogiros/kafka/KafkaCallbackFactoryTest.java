package com.gekalogiros.kafka;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Random;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class KafkaCallbackFactoryTest {

    @Mock
    private KafkaCallback kafkaCallback;

    @Mock
    private GenericRecord kafkaMessage;

    private KafkaCallbackFactory underTest;

    @BeforeEach
    void setUp() {
        this.underTest = new KafkaCallbackFactory(kafkaCallback);
    }

    @Test
    void create() {

        RecordMetadata recordMetadata = newRecordMetadata();

        underTest.create(kafkaMessage).onCompletion(recordMetadata, null);

        verify(kafkaCallback).onSuccess(kafkaMessage);
    }

    @Test
    void create_withError() {

        Callback callback = underTest.create(kafkaMessage);

        RuntimeException error = new RuntimeException();

        callback.onCompletion(null, error);

        verify(kafkaCallback).onError(kafkaMessage, error);
    }

    private RecordMetadata newRecordMetadata(){

        final Random random = new Random();

        return new RecordMetadata(
                new TopicPartition("topic", random.nextInt()),
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                random.nextInt(),
                random.nextInt());
    }
}