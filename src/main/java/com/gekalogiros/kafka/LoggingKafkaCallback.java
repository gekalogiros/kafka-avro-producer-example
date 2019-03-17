package com.gekalogiros.kafka;

import org.apache.avro.generic.GenericRecord;

public class LoggingKafkaCallback implements KafkaCallback {

    @Override
    public void onSuccess(GenericRecord genericRecord) {
        System.out.println("onSuccess");
        System.out.println(genericRecord);
    }

    @Override
    public void onError(GenericRecord genericRecord, Exception e) {
        System.out.println("onFailure");
        System.out.println(genericRecord);
        System.out.println(e);
    }
}
