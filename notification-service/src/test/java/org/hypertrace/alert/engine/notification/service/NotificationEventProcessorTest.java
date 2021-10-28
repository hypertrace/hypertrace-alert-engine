package org.hypertrace.alert.engine.notification.service;

import com.typesafe.config.Config;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.BaselineRuleViolationSummary;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.EventRecord;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyNotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.NotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.StaticRuleViolationSummary;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.StaticThresholdOperator;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.ViolationSummary;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.serviceframework.spi.PlatformServiceLifecycle;
import org.hypertrace.core.serviceframework.spi.PlatformServiceLifecycle.State;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

class NotificationEventProcessorTest {

  public static MockWebServer mockWebServer;

  @BeforeEach
  public void setUp() throws IOException {
    mockWebServer = new MockWebServer();
    mockWebServer.start(11502);
  }

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "notification-service")
  void testProcessForStaticThreshold() throws IOException {
    MockResponse mockedResponse =
        new MockResponse()
            .setResponseCode(200) // Sample
            .addHeader("Content-Type", "application/json");

    mockWebServer.enqueue(mockedResponse);
    // var httpUrl = mockWebServer.url("/hello/world").toString();

    List<ViolationSummary> violationSummaryList =
        List.of(
            ViolationSummary.newBuilder()
                .setViolationSummary(
                    StaticRuleViolationSummary.newBuilder()
                        .setStaticThreshold(1324)
                        .setMetricValues(List.of(1d, 2d))
                        .setOperator(StaticThresholdOperator.STATIC_THRESHOLD_OPERATOR_LT)
                        .setViolationCount(2)
                        .setDataCount(2)
                        .setEvaluationWindowDuration("PT5M")
                        .build())
                .build());

    NotificationEvent notificationEvent = getNotificationEvent(violationSummaryList);

    Assertions.assertEquals(0, mockWebServer.getRequestCount());
    Config config = ConfigClientFactory.getClient().getConfig();
    new NotificationEventProcessor(
            config.getConfig("notificationChannelsSource"),
            new PlatformServiceLifecycle() {
              @Override
              public CompletionStage<Void> shutdownComplete() {
                return new CompletableFuture().minimalCompletionStage();
              }

              @Override
              public State getState() {
                return null;
              }
            })
        .process(notificationEvent);
    Assertions.assertEquals(1, mockWebServer.getRequestCount());
    // verify body
    // RecordedRequest request = mockWebServer.takeRequest();
  }

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "notification-service")
  void testProcessForDynamicThreshold() throws IOException {
    MockResponse mockedResponse =
        new MockResponse()
            .setResponseCode(200) // Sample
            .addHeader("Content-Type", "application/json");

    mockWebServer.enqueue(mockedResponse);
    // var httpUrl = mockWebServer.url("/hello/world").toString();

    List<ViolationSummary> violationSummaryList =
        List.of(
            ViolationSummary.newBuilder()
                .setViolationSummary(
                    BaselineRuleViolationSummary.newBuilder()
                        .setBaselineUpperBound(100.0)
                        .setBaselineLowerBound(50.0)
                        .setMetricValues(List.of(123.0, 30.0, 150.0))
                        .setViolationCount(3)
                        .setDataCount(3)
                        .setEvaluationWindowDuration("PT5M")
                        .build())
                .build());

    NotificationEvent notificationEvent = getNotificationEvent(violationSummaryList);

    Assertions.assertEquals(0, mockWebServer.getRequestCount());
    Config config = ConfigClientFactory.getClient().getConfig();
    new NotificationEventProcessor(
            config.getConfig("notificationChannelsSource"),
            new PlatformServiceLifecycle() {
              @Override
              public CompletionStage<Void> shutdownComplete() {
                return new CompletableFuture().minimalCompletionStage();
              }

              @Override
              public State getState() {
                return null;
              }
            })
        .process(notificationEvent);
    Assertions.assertEquals(1, mockWebServer.getRequestCount());
  }

  private NotificationEvent getNotificationEvent(List<ViolationSummary> violationSummaries)
      throws IOException {
    MetricAnomalyNotificationEvent metricAnomalyNotificationEvent =
        MetricAnomalyNotificationEvent.newBuilder()
            .setChannelId("channel-id-1")
            .setEventConditionId("high-service-latency")
            .setViolationTimestamp(System.currentTimeMillis())
            .setEventConditionType("grth")
            .setViolationSummaryList(List.of())
            .setViolationSummaryList(violationSummaries)
            .build();

    EventRecord eventRecord =
        EventRecord.newBuilder()
            .setEventValue(metricAnomalyNotificationEvent.toByteBuffer())
            .setEventType(NotificationEventProcessor.METRIC_ANOMALY_ACTION_EVENT_TYPE)
            .setEventRecordMetadata(Map.of())
            .build();

    return NotificationEvent.newBuilder()
        .setEventRecord(eventRecord)
        .setNotificationEventMetadata(Map.of())
        .setTenantId("default")
        .setEventTimeMillis(System.currentTimeMillis())
        .build();
  }

  @AfterEach
  public void tearDown() throws IOException {
    mockWebServer.shutdown();
  }
}
