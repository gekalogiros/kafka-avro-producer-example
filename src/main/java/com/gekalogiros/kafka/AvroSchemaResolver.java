package com.gekalogiros.kafka;

import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;
import java.util.Set;

interface AvroSchemaResolver {

    Schema resolve();

    class SchemaRegistryOrFileFallbackResolver implements AvroSchemaResolver {

        private final SchemaRegistryClient schemaRegistryClient;
        private final FromSchemaRegistry schemaRegistryResolver;
        private final FromFile fileResolver;

        SchemaRegistryOrFileFallbackResolver(SchemaRegistryClient schemaRegistryClient,
                                             FromSchemaRegistry schemaRegistryResolver,
                                             FromFile fileResolver) {
            this.schemaRegistryClient = schemaRegistryClient;
            this.schemaRegistryResolver = schemaRegistryResolver;
            this.fileResolver = fileResolver;
        }

        @Override
        public Schema resolve() {
            final Set<String> subjects = schemaRegistryClient.getSubjects();
            return subjects.contains(schemaRegistryResolver.getSchemaName())
                    ? schemaRegistryResolver.resolve()
                    : fileResolver.resolve();
        }
    }

    class FromSchemaRegistry implements AvroSchemaResolver {

        private final SchemaRegistryClient schemaRegistryClient;
        private final String schemaName;

        FromSchemaRegistry(SchemaRegistryClient schemaRegistryClient, String schemaName) {
            this.schemaRegistryClient = schemaRegistryClient;
            this.schemaName = schemaName;
        }

        @Override
        public Schema resolve() {

            final String schemaDefinition = schemaRegistryClient.getLatestSchema(schemaName);

            return new Schema.Parser().parse(schemaDefinition);

        }

        String getSchemaName() {
            return schemaName;
        }
    }

    class FromFile implements AvroSchemaResolver {

        private final String path;

        FromFile(String path) {
            this.path = path;
        }

        @Override
        public Schema resolve() {
            try {
                return new Schema.Parser().parse(new File(path));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
