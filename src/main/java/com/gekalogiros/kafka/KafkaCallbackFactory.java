package com.gekalogiros.kafka;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;

import java.util.Optional;

class KafkaCallbackFactory {

    private final KafkaCallback kafkaCallback;

    KafkaCallbackFactory(KafkaCallback kafkaCallback) {
        this.kafkaCallback = kafkaCallback;
    }

    Callback create(GenericRecord message) {
        return (recordMetadata, e) ->
                Optional.ofNullable(e).ifPresentOrElse(
                        error -> kafkaCallback.onError(message, error),
                        () -> kafkaCallback.onSuccess(message)
                );
    }
}
