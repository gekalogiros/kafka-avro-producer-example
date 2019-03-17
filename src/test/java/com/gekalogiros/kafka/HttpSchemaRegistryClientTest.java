package com.gekalogiros.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.util.Set;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HttpSchemaRegistryClientTest {

    private static final String SUBJECT = "subject";
    private static final String SCHEMA = "a schema";
    private static final String SCHEMA_RESPONSE = "{\"subject\":\"" + SUBJECT + "\", \"id\":1, \"version\":1, \"schema\":\"" + SCHEMA + "\"}";

    private WireMockServer wireMockServer;

    private SchemaRegistryClient underTest;

    @BeforeEach
    void setup() {

        wireMockServer = new WireMockServer(0);
        wireMockServer.start();
        setupStub();

        this.underTest = new SchemaRegistryClient.HttpSchemaRegistryClient(
                HttpClient.newHttpClient(),
                new Serialization.JacksonSerialization(new ObjectMapper()),
                String.format("http://localhost:%d", wireMockServer.port())
        );

        assertTrue(wireMockServer.isRunning());
    }

    @AfterEach
    void tearDown() {
        wireMockServer.stop();
    }

    @Test
    void getSubjects() {

        Set<String> subjects = underTest.getSubjects();

        assertEquals(subjects.size(), 1);
        assertTrue(subjects.contains(SUBJECT));
    }

    @Test
    void getLatestSchema() {

        String schema = underTest.getLatestSchema(SUBJECT);

        assertEquals(schema, SCHEMA);
    }

    void setupStub() {

        final ResponseDefinitionBuilder responseDefinitionBuilder = aResponse()
                .withHeader("Content-Type", "application/json")
                .withStatus(200);

        wireMockServer.stubFor(get(urlEqualTo("/subjects"))
                .willReturn(responseDefinitionBuilder.withBody("[\"" + SUBJECT + "\"]")));

        wireMockServer.stubFor(get(urlEqualTo(String.format("/subjects/%s/versions/latest", SUBJECT)))
                .willReturn(responseDefinitionBuilder.withBody(SCHEMA_RESPONSE)));
    }
}