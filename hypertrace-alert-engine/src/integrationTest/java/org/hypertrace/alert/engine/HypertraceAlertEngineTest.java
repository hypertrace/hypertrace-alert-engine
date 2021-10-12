package org.hypertrace.alert.engine;

import static com.github.stefanbirkner.systemlambda.SystemLambda.withEnvironmentVariable;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.typesafe.config.ConfigFactory;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.reflect.Type;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.hypertrace.core.attribute.service.client.AttributeServiceClient;
import org.hypertrace.core.attribute.service.v1.AttributeMetadataFilter;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde;
import org.hypertrace.core.serviceframework.IntegrationTestServerUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class HypertraceAlertEngineTest {

  private static final Logger LOG = LoggerFactory.getLogger(HypertraceAlertEngineTest.class);

  private static final Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(LOG);
  private static final Map<String, String> TENANT_ID_MAP = Map.of("x-tenant-id", "__default");
  private static final int CONTAINER_STARTUP_ATTEMPTS = 5;
  private static final int NOTIFICATION_CHANNEL_PORT1 = 11502;
  private static final int NOTIFICATION_CHANNEL_PORT2 = 11505;

  private static AdminClient adminClient;
  private static String bootstrapServers;

  private static Network network;
  private static GenericContainer<?> mongo;
  private static GenericContainer<?> attributeService;
  private static GenericContainer<?> queryService;
  private static KafkaContainer kafkaZk;
  private static GenericContainer<?> pinotServiceManager;

  @BeforeAll
  public static void setup() throws Exception {
    network = org.testcontainers.containers.Network.newNetwork();

    kafkaZk =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))
            .withNetwork(network)
            .withNetworkAliases("kafka", "zookeeper")
            .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
            .waitingFor(Wait.forListeningPort());
    kafkaZk.start();

    pinotServiceManager =
        new GenericContainer<>(DockerImageName.parse("hypertrace/pinot-servicemanager:main"))
            .withNetwork(network)
            .withNetworkAliases("pinot-controller", "pinot-server", "pinot-broker")
            .withExposedPorts(8099)
            .withExposedPorts(9000)
            .dependsOn(kafkaZk)
            .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
            .waitingFor(Wait.forLogMessage(".*Completed schema installation.*", 1))
            .withLogConsumer(logConsumer);
    pinotServiceManager.start();

    mongo =
        new GenericContainer<>(DockerImageName.parse("hypertrace/mongodb:main"))
            .withNetwork(network)
            .withNetworkAliases("mongo")
            .withExposedPorts(27017)
            .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
            .waitingFor(Wait.forLogMessage(".*waiting for connections on port 27017.*", 1));
    mongo.start();
    mongo.followOutput(logConsumer);

    attributeService =
        new GenericContainer<>(DockerImageName.parse("hypertrace/attribute-service:main"))
            .withNetwork(network)
            .withNetworkAliases("attribute-service")
            .withEnv("MONGO_HOST", "mongo")
            .withExposedPorts(9012)
            .dependsOn(mongo)
            .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
            .waitingFor(Wait.forLogMessage(".*Started admin service on port: 9013.*", 1));
    attributeService.start();
    attributeService.followOutput(logConsumer);

    queryService =
        new GenericContainer<>(DockerImageName.parse("hypertrace/query-service:main"))
            .withNetwork(network)
            .withNetworkAliases("query-service")
            .withEnv("ATTRIBUTE_SERVICE_HOST_CONFIG", attributeService.getHost())
            .withEnv(
                "ATTRIBUTE_SERVICE_PORT_CONFIG", attributeService.getMappedPort(9012).toString())
            .withEnv("PINOT_CONNECTION_TYPE", "broker")
            .withEnv("ZK_CONNECT_STR", "pinot-broker:8099")
            .withExposedPorts(8090)
            .dependsOn(attributeService)
            .dependsOn(pinotServiceManager)
            .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
            .waitingFor(Wait.forLogMessage(".*Started admin service on port: 8091.*", 1));
    queryService.start();
    queryService.followOutput(logConsumer);

    List<String> topicsNames =
        List.of(
            "enriched-structured-traces",
            "raw-service-view-events",
            "raw-trace-view-events",
            "service-call-view-events",
            "span-event-view",
            "backend-entity-view-events",
            "log-event-view",
            "raw-logs");
    bootstrapServers = kafkaZk.getBootstrapServers();
    adminClient =
        AdminClient.create(
            Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaZk.getBootstrapServers()));
    List<NewTopic> topics =
        topicsNames.stream().map(v -> new NewTopic(v, 1, (short) 1)).collect(Collectors.toList());
    adminClient.createTopics(topics);

    assertTrue(bootstrapConfig());
    LOG.info("Bootstrap Complete");
    long traceTimeStamp = System.currentTimeMillis();
    LOG.info("TraceTimeStamp for trace is {}", Instant.ofEpochMilli(traceTimeStamp));
    assertTrue(generateData(traceTimeStamp));
    LOG.info("Generate Data Complete");
  }

  @AfterAll
  public static void shutdown() {
    LOG.info("Initiating shutdown");
    attributeService.stop();
    queryService.stop();
    mongo.stop();
    pinotServiceManager.stop();
    kafkaZk.stop();
    network.close();
  }

  @Test
  public void testStaticThresholdNotificationSent() throws Exception {
    LOG.info("Starting testStaticThresholdNotificationSent");
    MockWebServer mockWebServer = new MockWebServer();
    mockWebServer.start(NOTIFICATION_CHANNEL_PORT1);
    MockResponse mockedResponse =
        new MockResponse().setResponseCode(200).addHeader("Content-Type", "application/json");
    mockWebServer.enqueue(mockedResponse);

    Assertions.assertEquals(0, mockWebServer.getRequestCount());
    withEnvironmentVariable("ATTRIBUTE_SERVICE_HOST_CONFIG", attributeService.getHost())
        .and("ATTRIBUTE_SERVICE_PORT_CONFIG", attributeService.getMappedPort(9012).toString())
        .and("QUERY_SERVICE_HOST_CONFIG", queryService.getHost())
        .and("QUERY_SERVICE_PORT_CONFIG", queryService.getMappedPort(8090).toString())
        .and("ALERT_RULES_PATH", "build/resources/integrationTest/alert-rules-static-rule.json")
        .and(
            "NOTIFICATION_CHANNELS_PATH",
            "build/resources/integrationTest/notification-channels-1.json")
        .and("SERVICE_ADMIN_PORT", "10005")
        .and("JOB_SUFFIX", "job1")
        .execute(
            () -> {
              ConfigFactory.invalidateCaches();
              IntegrationTestServerUtil.startServices(new String[] {"hypertrace-alert-engine"});
            });
    int retryCount = 0;
    while (!(mockWebServer.getRequestCount() > 0) && retryCount++ < 10) {
      Thread.sleep(Duration.ofSeconds(30).toMillis());
    }
    Assertions.assertTrue(retryCount < 10);
    String notificationBody1 =
        mockWebServer.takeRequest().getBody().readString(Charset.defaultCharset());
    LOG.info(
        "Static Threshold Test, No of retries {}, staticThreshold notification {}",
        retryCount,
        notificationBody1);

    Assertions.assertTrue(notificationBody1.contains("static threshold"));
    mockWebServer.close();

    IntegrationTestServerUtil.shutdownServices();
  }

  @Test
  public void testDynamicThresholdNotificationSent() throws Exception {
    LOG.info("Starting testDynamicThresholdNotificationSent");
    MockWebServer mockWebServer = new MockWebServer();
    mockWebServer.start(NOTIFICATION_CHANNEL_PORT2);
    MockResponse mockedResponse =
        new MockResponse().setResponseCode(200).addHeader("Content-Type", "application/json");
    mockWebServer.enqueue(mockedResponse);

    withEnvironmentVariable("ATTRIBUTE_SERVICE_HOST_CONFIG", attributeService.getHost())
        .and("ATTRIBUTE_SERVICE_PORT_CONFIG", attributeService.getMappedPort(9012).toString())
        .and("QUERY_SERVICE_HOST_CONFIG", queryService.getHost())
        .and("QUERY_SERVICE_PORT_CONFIG", queryService.getMappedPort(8090).toString())
        .and("ALERT_RULES_PATH", "build/resources/integrationTest/alert-rules-dynamic-rule.json")
        .and(
            "NOTIFICATION_CHANNELS_PATH",
            "build/resources/integrationTest/notification-channels-2.json")
        .and("SERVICE_ADMIN_PORT", "10010")
        .and("JOB_SUFFIX", "job2")
        .execute(
            () -> {
              ConfigFactory.invalidateCaches();
              IntegrationTestServerUtil.startServices(new String[] {"hypertrace-alert-engine"});
            });

    Assertions.assertEquals(0, mockWebServer.getRequestCount());
    // int x = 1;
    int retryCount = 0;
    // no notification sent
    while (retryCount++ < 5) {
      Assertions.assertEquals(mockWebServer.getRequestCount(), 0);
      Thread.sleep(Duration.ofSeconds(30).toMillis());
    }
    mockWebServer.close();

    IntegrationTestServerUtil.shutdownServices();
  }

  private static boolean bootstrapConfig() throws Exception {
    GenericContainer<?> bootstrapper =
        new GenericContainer<>(DockerImageName.parse("hypertrace/config-bootstrapper:main"))
            .withNetwork(network)
            .dependsOn(attributeService)
            .withEnv("MONGO_HOST", "mongo")
            .withEnv("ATTRIBUTE_SERVICE_HOST_CONFIG", "attribute-service")
            .withCommand(
                "-c",
                "/app/resources/configs/config-bootstrapper/application.conf",
                "-C",
                "/app/resources/configs/config-bootstrapper/attribute-service",
                "--upgrade")
            .withLogConsumer(logConsumer);
    bootstrapper.start();

    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(
                attributeService.getHost(), attributeService.getMappedPort(9012))
            .usePlaintext()
            .build();
    AttributeServiceClient client = new AttributeServiceClient(channel);
    int retry = 0;
    while (Streams.stream(
                    client.findAttributes(
                        TENANT_ID_MAP, AttributeMetadataFilter.getDefaultInstance()))
                .collect(Collectors.toList())
                .size()
            == 0
        && retry++ < 5) {
      Thread.sleep(2000);
    }
    channel.shutdown();
    bootstrapper.stop();
    return retry < 5;
  }

  private static boolean generateData(long timeStamp) throws Exception {
    // start view-gen service
    GenericContainer<?> viewGen =
        new GenericContainer<>(DockerImageName.parse("hypertrace/hypertrace-view-generator:main"))
            .withNetwork(network)
            .dependsOn(kafkaZk)
            .withEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
            .withEnv(
                "DEFAULT_KEY_SERDE", "org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde")
            .withEnv(
                "DEFAULT_VALUE_SERDE",
                "org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde")
            .withEnv("NUM_STREAM_THREADS", "1")
            .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
            .waitingFor(Wait.forLogMessage(".* Started admin service on port: 8099.*", 1));
    viewGen.start();
    viewGen.followOutput(logConsumer);

    // produce data
    StructuredTrace trace = getTrace();
    updateTraceTimeStamp(trace, timeStamp);

    KafkaProducer<String, StructuredTrace> producer =
        new KafkaProducer<>(
            ImmutableMap.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers,
                ProducerConfig.CLIENT_ID_CONFIG,
                UUID.randomUUID().toString()),
            new StringSerializer(),
            new AvroSerde<StructuredTrace>().serializer());
    producer.send(new ProducerRecord<>("enriched-structured-traces", "", trace)).get();

    Map<String, Long> endOffSetMap =
        Map.of(
            "raw-service-view-events", 13L,
            "backend-entity-view-events", 11L,
            "raw-trace-view-events", 1L,
            "service-call-view-events", 20L,
            "span-event-view", 50L,
            "log-event-view", 0L);
    int retry = 0;
    while (!areMessagesConsumed(endOffSetMap) && retry++ < 5) {
      Thread.sleep(2000);
    }
    // stop this service
    viewGen.stop();

    return retry < 5;
  }

  private static boolean areMessagesConsumed(Map<String, Long> endOffSetMap) throws Exception {
    ListConsumerGroupOffsetsResult consumerGroupOffsetsResult =
        adminClient.listConsumerGroupOffsets("");
    Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap =
        consumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get();
    if (offsetAndMetadataMap.size() < 6) {
      return false;
    }
    return offsetAndMetadataMap.entrySet().stream()
        .noneMatch(k -> k.getValue().offset() < endOffSetMap.get(k.getKey().topic()));
  }

  private static void updateTraceTimeStamp(StructuredTrace trace, long timeStamp) {
    long startTime = timeStamp - Duration.ofSeconds(5).toMillis();
    trace.setStartTimeMillis(timeStamp);
    trace.setEndTimeMillis(timeStamp);
    // update events
    trace.getEventList().forEach(e -> e.setStartTimeMillis(startTime));
    trace.getEventList().forEach(e -> e.setEndTimeMillis(timeStamp));
    // updates edges
    trace.getEntityEdgeList().forEach(edge -> edge.setStartTimeMillis(startTime));
    trace.getEntityEdgeList().forEach(edge -> edge.setEndTimeMillis(timeStamp));
    trace.getEventEdgeList().forEach(edge -> edge.setStartTimeMillis(startTime));
    trace.getEventEdgeList().forEach(edge -> edge.setEndTimeMillis(timeStamp));
    trace.getEntityEventEdgeList().forEach(edge -> edge.setStartTimeMillis(startTime));
    trace.getEntityEventEdgeList().forEach(edge -> edge.setEndTimeMillis(timeStamp));
  }

  private static StructuredTrace getTrace() throws FileNotFoundException {
    Gson gson =
        new GsonBuilder()
            .serializeNulls()
            .registerTypeHierarchyAdapter(ByteBuffer.class, new ByteBufferTypeAdapter())
            .create();
    URL resource =
        Thread.currentThread().getContextClassLoader().getResource("StructuredTrace-Hotrod.json");
    return gson.fromJson(new FileReader(resource.getPath()), StructuredTrace.class);
  }

  private static class ByteBufferTypeAdapter
      implements JsonDeserializer<ByteBuffer>, JsonSerializer<ByteBuffer> {

    @Override
    public ByteBuffer deserialize(
        JsonElement jsonElement, Type type, JsonDeserializationContext context) {
      return ByteBuffer.wrap(Base64.decodeBase64(jsonElement.getAsString()));
    }

    @Override
    public JsonElement serialize(ByteBuffer src, Type typeOfSrc, JsonSerializationContext context) {
      return new JsonPrimitive(Base64.encodeBase64String(src.array()));
    }
  }
}
