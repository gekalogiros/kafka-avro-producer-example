package com.gekalogiros.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class Application {

    private static final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public static void main(String[] args) throws InterruptedException {

        final ApplicationConfig applicationConfig = new ApplicationConfig(new ResourceLoadableProperties("/application.properties").load());

        final SchemaRegistryClient schemaRegistryClient = new SchemaRegistryClient.HttpSchemaRegistryClient(
                HttpClient.newHttpClient(),
                new Serialization.JacksonSerialization(new ObjectMapper()),
                applicationConfig.getSchemaRegistryUrl());

        final AvroSchemaResolver avroSchemaResolver = new AvroSchemaResolver.SchemaRegistryOrFileFallbackResolver(
                schemaRegistryClient,
                new AvroSchemaResolver.FromSchemaRegistry(schemaRegistryClient, applicationConfig.getSchemaName()),
                new AvroSchemaResolver.FromFile(applicationConfig.getSchemaDefinitionResource())
        );

        final Properties kafkaProducerProperties = new ResourceLoadableProperties("/kafka-producer.properties").load();

        final KafkaCallbackFactory kafkaCallbackFactory = new KafkaCallbackFactory(new LoggingKafkaCallback());

        // KAFKA PRODUCER

        final KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(kafkaProducerProperties);

        final ProducerRecordFactory producerRecordFactory = new ProducerRecordFactory(applicationConfig.getTopic(), avroSchemaResolver.resolve());

        final Consumer<ProducerRecord<String, GenericRecord>> publisher =
                record -> producer.send(record, kafkaCallbackFactory.create(record.value()));

        publisher.accept(producerRecordFactory.fromData());

        publisher.accept(producerRecordFactory.fromObject());

        publisher.accept(producerRecordFactory.fromJson());

        // KAFKA CONSUMER

        final Properties kafkaConsumerProperties = new ResourceLoadableProperties("/kafka-consumer.properties").load();

        final KafkaConsumer<String, KafkaMessage> consumer = new KafkaConsumer<>(kafkaConsumerProperties);

        consumer.subscribe(Collections.singletonList(applicationConfig.getTopic()));
        consumer.poll(Duration.ofMillis(10000)).forEach(e -> System.out.println(e.value()));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executorService.submit(() -> {
                producer.flush();
                producer.close();
                consumer.close();
            });
        }));

        executorService.awaitTermination(applicationConfig.getShutdownWaitingTimeInMilliseconds(), TimeUnit.MILLISECONDS);
    }
}
