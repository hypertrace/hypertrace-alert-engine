package org.hypertrace.alert.engine.metric.anomaly.task.manager;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask.Builder;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.queue.KafkaAlertTaskConsumer;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskConverter;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobManager;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.DbRuleSource;
import org.hypertrace.alert.engine.metric.anomaly.task.manager.job.JobManager;
import org.hypertrace.alerting.config.service.v1.Attribute;
import org.hypertrace.alerting.config.service.v1.BaselineThresholdCondition;
import org.hypertrace.alerting.config.service.v1.CreateEventConditionRequest;
import org.hypertrace.alerting.config.service.v1.EventCondition;
import org.hypertrace.alerting.config.service.v1.EventConditionConfigServiceGrpc;
import org.hypertrace.alerting.config.service.v1.EventConditionMutableData;
import org.hypertrace.alerting.config.service.v1.Filter;
import org.hypertrace.alerting.config.service.v1.LeafFilter;
import org.hypertrace.alerting.config.service.v1.LhsExpression;
import org.hypertrace.alerting.config.service.v1.MetricAggregationFunction;
import org.hypertrace.alerting.config.service.v1.MetricAnomalyEventCondition;
import org.hypertrace.alerting.config.service.v1.MetricSelection;
import org.hypertrace.alerting.config.service.v1.NewEventCondition;
import org.hypertrace.alerting.config.service.v1.RhsExpression;
import org.hypertrace.alerting.config.service.v1.ValueOperator;
import org.hypertrace.alerting.config.service.v1.ViolationCondition;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.spi.PlatformServiceLifecycle;
import org.hypertrace.notification.config.service.v1.CreateNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.NotificationRule;
import org.hypertrace.notification.config.service.v1.NotificationRuleConfigServiceGrpc;
import org.hypertrace.notification.config.service.v1.NotificationRuleMutableData;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
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
public class TaskManagerTest {

  private static final Logger LOG = LoggerFactory.getLogger(TaskManagerTest.class);
  private static final String EVENT_CONDITION_TYPE = "MetricAnomalyEventCondition";
  private static final int CONTAINER_STARTUP_ATTEMPTS = 5;
  private static final Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(LOG);

  private static Network network;
  private static KafkaContainer kafkaZk;
  private static GenericContainer<?> mongo;
  private static GenericContainer<?> configService;
  private static ManagedChannel managedChannel;

  @BeforeAll
  public static void setup() throws Exception {
    network = Network.newNetwork();

    kafkaZk =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))
            .withNetwork(network)
            .withNetworkAliases("kafka", "zookeeper")
            .withExposedPorts(9093)
            .withStartupAttempts(2)
            .waitingFor(Wait.forListeningPort());
    kafkaZk.start();

    mongo =
        new GenericContainer<>(DockerImageName.parse("mongo:4.2.0"))
            .withNetwork(network)
            .withNetworkAliases("mongo")
            .withExposedPorts(27017)
            .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
            .waitingFor(Wait.forListeningPort())
            .withLogConsumer(logConsumer);
    mongo.start();

    configService =
        new GenericContainer<>(DockerImageName.parse("hypertrace/config-service:main"))
            .withNetwork(network)
            .withNetworkAliases("config-service")
            .withEnv("MONGO_HOST", "mongo")
            .withExposedPorts(50101)
            .dependsOn(mongo)
            .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
            .waitingFor(Wait.forLogMessage(".*Started admin service on port: 50102.*", 1));
    configService.start();
    configService.followOutput(logConsumer);

    managedChannel =
        ManagedChannelBuilder.forAddress("localhost", configService.getMappedPort(50101))
            .usePlaintext()
            .build();
  }

  @AfterAll
  public static void shutDown() {
    kafkaZk.stop();
    mongo.stop();
    configService.stop();
    network.close();
    managedChannel.shutdown();
  }

  @Test
  public void testReadRulesFromDb() throws IOException {
    // first create rules
    NotificationRuleConfigServiceGrpc.NotificationRuleConfigServiceBlockingStub
        notificationRuleStub =
            NotificationRuleConfigServiceGrpc.newBlockingStub(managedChannel)
                .withCallCredentials(
                    RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider()
                        .get());
    EventConditionConfigServiceGrpc.EventConditionConfigServiceBlockingStub eventConditionStub =
        EventConditionConfigServiceGrpc.newBlockingStub(managedChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    RequestContext context = RequestContext.forTenantId("tenant-1");

    EventCondition eventCondition =
        context
            .call(
                () ->
                    eventConditionStub.createEventCondition(
                        CreateEventConditionRequest.newBuilder()
                            .setNewEventCondition(
                                NewEventCondition.newBuilder()
                                    .setEventConditionData(
                                        EventConditionMutableData.newBuilder()
                                            .setMetricAnomalyEventCondition(
                                                getMetricAnomalyEventCondition("PT1M"))
                                            .build())
                                    .build())
                            .build()))
            .getEventCondition();

    NotificationRuleMutableData notificationRuleMutableData =
        NotificationRuleMutableData.newBuilder()
            .setRuleName("rule-1")
            .setDescription("sample rule")
            .setChannelId("channel-1")
            .setEventConditionType("metricAnomalyEventCondition")
            .setEventConditionId(eventCondition.getId())
            .build();

    NotificationRule notificationRule =
        context
            .call(
                () ->
                    notificationRuleStub.createNotificationRule(
                        CreateNotificationRuleRequest.newBuilder()
                            .setNotificationRuleMutableData(notificationRuleMutableData)
                            .build()))
            .getNotificationRule();

    Map<String, String> testConfigMap =
        Map.of(
            "alertRuleSource.dataStore.config.service.port",
            String.valueOf(configService.getMappedPort(50101)));

    URL resourceUrl =
        Thread.currentThread().getContextClassLoader().getResource("application.conf");
    Config appConfig =
        ConfigFactory.parseMap(testConfigMap)
            .withFallback(ConfigFactory.parseURL(resourceUrl))
            .resolve();

    DbRuleSource dbRuleSource =
        new DbRuleSource(
            appConfig.getConfig("alertRuleSource.dataStore"),
            new PlatformServiceLifecycle() {
              @Override
              public CompletionStage<Void> shutdownComplete() {
                return new CompletableFuture().minimalCompletionStage();
              }

              @Override
              public State getState() {
                return null;
              }
            });

    List<Document> documentList = dbRuleSource.getAllRules(null);
    Optional<Builder> builderOptional =
        new AlertTaskConverter(appConfig).toAlertTaskBuilder(documentList.get(0));
    Assertions.assertTrue(builderOptional.isPresent());
    Assertions.assertEquals(
        notificationRule.getNotificationRuleMutableData().getChannelId(),
        builderOptional.get().getChannelId());
  }

  @Test
  public void testAlertTask() throws SchedulerException, URISyntaxException {
    Integer port = kafkaZk.getMappedPort(9093);
    URL resourceUrl =
        Thread.currentThread().getContextClassLoader().getResource("application.conf");
    URL pathUrl = Thread.currentThread().getContextClassLoader().getResource("rules.json");
    File file = Paths.get(pathUrl.toURI()).toFile();
    String absolutePath = file.getAbsolutePath();

    Map<String, String> testConfigMap =
        Map.of(
            "queue.config.kafka.bootstrap.servers",
            "localhost:" + port,
            "alertRuleSource.fs.path",
            absolutePath);
    Config appConfig =
        ConfigFactory.parseMap(testConfigMap)
            .withFallback(ConfigFactory.parseURL(resourceUrl))
            .resolve();

    // start the job
    SchedulerFactory schedulerFactory = new StdSchedulerFactory();
    Scheduler scheduler = schedulerFactory.getScheduler();
    JobManager jobManager =
        new AlertTaskJobManager(
            new PlatformServiceLifecycle() {
              @Override
              public CompletionStage<Void> shutdownComplete() {
                return new CompletableFuture().minimalCompletionStage();
              }

              @Override
              public State getState() {
                return null;
              }
            });
    jobManager.initJob(appConfig);

    jobManager.startJob(scheduler);
    scheduler.start();

    // start the consumer that will wait max 30 seconds
    Config kafkaQueueConfig = appConfig.getConfig("queue.config.kafka");
    KafkaAlertTaskConsumer kafkaAlertTaskConsumer = new KafkaAlertTaskConsumer(kafkaQueueConfig);
    boolean result = startConsumerLoop(kafkaAlertTaskConsumer);

    jobManager.stopJob(scheduler);
    scheduler.shutdown();

    // at least we found 1 message, and were able to parse it successfully
    Assertions.assertTrue(result);
  }

  private boolean startConsumerLoop(KafkaAlertTaskConsumer kafkaAlertTaskConsumer) {
    int sleepTimeMs = 1000; // 1 seconds
    int maxWaitTimeMs = 30 * sleepTimeMs; // wait for max 30 seconds
    int elapsedWaitTime = 0;
    while (true) {
      try {
        AlertTask alertTask = kafkaAlertTaskConsumer.dequeue();
        if (alertTask != null && alertTask.getEventConditionType().equals(EVENT_CONDITION_TYPE)) {
          MetricAnomalyEventCondition.parseFrom(alertTask.getEventConditionValue());
          return true;
        } else {
          Thread.sleep(sleepTimeMs);
          elapsedWaitTime += sleepTimeMs;
        }

        if (elapsedWaitTime > maxWaitTimeMs) {
          return false;
        }
      } catch (InterruptedException | IOException e) {
        e.printStackTrace();
      }
    }
  }

  private MetricAnomalyEventCondition getMetricAnomalyEventCondition(
      String evaluationWindowDuration) {
    LhsExpression lhsExpression =
        LhsExpression.newBuilder()
            .setAttribute(Attribute.newBuilder().setKey("name").setScope("SERVICE").build())
            .build();
    RhsExpression rhsExpression = RhsExpression.newBuilder().setStringValue("frontend").build();
    LeafFilter leafFilter =
        LeafFilter.newBuilder()
            .setValueOperator(ValueOperator.VALUE_OPERATOR_EQ)
            .setLhsExpression(lhsExpression)
            .setRhsExpression(rhsExpression)
            .build();
    MetricSelection metricSelection =
        MetricSelection.newBuilder()
            .setMetricAggregationInterval("PT1M")
            .setMetricAggregationFunction(
                MetricAggregationFunction.METRIC_AGGREGATION_FUNCTION_TYPE_AVGRATE)
            .setFilter(Filter.newBuilder().setLeafFilter(leafFilter).build())
            .setMetricAttribute(
                Attribute.newBuilder().setKey("errorCount").setScope("SERVICE").build())
            .build();

    MetricAnomalyEventCondition.Builder metricAnomalyEventConditionBuilder =
        MetricAnomalyEventCondition.newBuilder();
    metricAnomalyEventConditionBuilder.setEvaluationWindowDuration(evaluationWindowDuration);
    metricAnomalyEventConditionBuilder.setMetricSelection(metricSelection);
    metricAnomalyEventConditionBuilder.addViolationCondition(
        ViolationCondition.newBuilder()
            .setBaselineThresholdCondition(
                BaselineThresholdCondition.newBuilder().setBaselineDuration("PT5M").build())
            .build());
    return metricAnomalyEventConditionBuilder.build();
  }
}
