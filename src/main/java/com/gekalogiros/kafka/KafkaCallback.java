package com.gekalogiros.kafka;

import org.apache.avro.generic.GenericRecord;

public interface KafkaCallback {

    void onSuccess(GenericRecord genericRecord);

    void onError(GenericRecord genericRecord, Exception e);
}
