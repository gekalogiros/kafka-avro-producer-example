package com.gekalogiros.kafka;

import java.util.Properties;

class ApplicationConfig {

    private final Properties properties;

    ApplicationConfig(Properties properties) {
        this.properties = properties;
    }

    String getSchemaRegistryUrl() {
        return properties.getProperty("avro.schema.registry");
    }

    String getSchemaDefinitionResource() {
        return properties.getProperty("avro.schema.location");
    }

    String getSchemaName() {
        return properties.getProperty("avro.schema.name");
    }

    String getTopic() {
        return properties.getProperty("kafka.topic");
    }

    int getShutdownWaitingTimeInMilliseconds() {
        return Integer.valueOf(properties.getProperty("shutdown.waiting.time.milliseconds"));
    }
}
