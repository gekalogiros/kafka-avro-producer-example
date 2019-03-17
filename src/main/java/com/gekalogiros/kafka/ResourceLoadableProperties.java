package com.gekalogiros.kafka;

import java.io.IOException;
import java.util.Properties;

class ResourceLoadableProperties extends Properties {

    private final String resource;

    ResourceLoadableProperties(final String resource) {
        super();
        this.resource = resource;
    }

    Properties load() {
        try {
            load(this.getClass().getResourceAsStream(resource));
        }
        catch (IOException e) {
            throw new RuntimeException(String.format("Failed to load properties %s", resource));
        }
        return this;
    }
}
