package com.gekalogiros.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

interface SchemaRegistryClient {

    Set<String> getSubjects();

    String getLatestSchema(String subject);

    @SuppressWarnings("unchecked")
    class HttpSchemaRegistryClient implements SchemaRegistryClient {

        private static final Logger LOG = LoggerFactory.getLogger(HttpSchemaRegistryClient.class);

        private final HttpClient httpClient;
        private final Serialization serialization;
        private final String schemaRegistryUrl;

        HttpSchemaRegistryClient(HttpClient httpClient, Serialization serialization, String schemaRegistryUrl) {
            this.httpClient = httpClient;
            this.serialization = serialization;
            this.schemaRegistryUrl = schemaRegistryUrl;
        }

        @Override
        public Set<String> getSubjects() {

            HttpRequest request = createRequest(String.format("%s/subjects", schemaRegistryUrl));

            return (Set<String>) get(request, Set.class).join();
        }

        @Override
        public String getLatestSchema(String subject) {

            HttpRequest request = createRequest(String.format("%s/subjects/%s/versions/latest", schemaRegistryUrl, subject));

            return get(request, SchemaResponse.class).join().schema;
        }

        private <T> CompletableFuture<T> get(HttpRequest httpRequest, Class<T> clazz) {
            return httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
                    .thenApply(HttpResponse::body)
                    .thenApply(s -> {
                        LOG.info("Schema Registry Response {}", s);
                        return serialization.from(s, clazz);
                    });
        }

        private HttpRequest createRequest(String url) {
            return HttpRequest.newBuilder()
                    .GET()
                    .header("Accept", "application/json")
                    .uri(URI.create(url))
                    .build();
        }
    }
}
