package org.hypertrace.alert.engine.notification.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.EventRecord;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyNotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.NotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.Operator;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.ViolationSummary;
import org.hypertrace.alert.engine.notification.service.NotificationChannel.WebFormatNotificationChannelConfig;
import org.hypertrace.alert.engine.notification.service.notification.WebhookNotifier;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class NotificationEventProcessorTest {

  @Test
  void testProcess() throws IOException {
    NotificationChannel notificationChannel =
        NotificationChannel.builder()
            .channelName("1")
            .channelId("1")
            .notificationChannelConfig(
                List.of(
                    WebFormatNotificationChannelConfig.builder()
                        .channelConfigType(NotificationChannelsReader.CHANNEL_CONFIG_TYPE_WEBHOOK)
                        .url("https://hooks.slack.com/services/abc")
                        .webhookFormat(NotificationChannelsReader.WEBHOOK_FORMAT_SLACK)
                        .build()))
            .build();

    List<ViolationSummary> violationSummaryList =
        List.of(
            ViolationSummary.newBuilder()
                .setRhs(1324)
                .setLhs(List.of(1d, 2d))
                .setOperator(Operator.STATIC_THRESHOLD_OPERATOR_LT)
                .setViolationCount(2)
                .setDataCount(2)
                .build());

    MetricAnomalyNotificationEvent metricAnomalyNotificationEvent =
        MetricAnomalyNotificationEvent.newBuilder()
            .setChannelId("1")
            .setEventConditionId("5")
            .setViolationTimestamp(System.currentTimeMillis())
            .setEventConditionType("grth")
            .setViolationSummaryList(List.of())
            .setViolationSummaryList(violationSummaryList)
            .build();

    EventRecord eventRecord =
        EventRecord.newBuilder()
            .setEventValue(metricAnomalyNotificationEvent.toByteBuffer())
            .setEventType(NotificationEventProcessor.METRIC_ANOMALY_ACTION_EVENT_TYPE)
            .setEventRecordMetadata(Map.of())
            .build();

    NotificationEvent notificationEvent =
        NotificationEvent.newBuilder()
            .setEventRecord(eventRecord)
            .setNotificationEventMetadata(Map.of())
            .setTenantId("tenant-1")
            .setEventTimeMillis(System.currentTimeMillis())
            .build();

    WebhookNotifier webhookNotifier = Mockito.mock(WebhookNotifier.class);

    new NotificationEventProcessor(List.of(notificationChannel), webhookNotifier)
        .process(notificationEvent);

    Mockito.verify(webhookNotifier).notify(Mockito.any(), Mockito.any());
  }
}
