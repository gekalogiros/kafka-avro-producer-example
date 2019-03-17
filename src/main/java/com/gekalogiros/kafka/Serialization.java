package com.gekalogiros.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

interface Serialization {

    <T> T from(String json, Class<T> clazz);

    class JacksonSerialization implements Serialization {

        private final ObjectMapper objectMapper;

        JacksonSerialization(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @Override
        public <T> T from(String json, Class<T> clazz) {
            try {
                return objectMapper.readValue(json, clazz);
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
    }
}
