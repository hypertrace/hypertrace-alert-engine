package org.hypertrace.alert.engine.anomaly.event.processor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.hypertrace.alert.engine.anomaly.event.processor.NotificationChannel.WebFormatNotificationChannelConfig;
import org.hypertrace.alert.engine.anomaly.event.processor.notification.WebhookNotifier;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.ActionEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.EventRecord;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyViolation;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class AnomalyEventProcessorTest {

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
                        .url("https://hooks.slack.com/services/abcd")
                        .webhookFormat(NotificationChannelsReader.WEBHOOK_FORMAT_SLACK)
                        .build()))
            .build();

    MetricAnomalyViolation metricAnomalyViolation =
        MetricAnomalyViolation.newBuilder()
            .setChannelId("1")
            .setEventConditionId("5")
            .setViolationTimestamp(System.currentTimeMillis())
            .setEventConditionType("grth")
            .build();

    EventRecord eventRecord =
        EventRecord.newBuilder()
            .setEventValue(metricAnomalyViolation.toByteBuffer())
            .setEventType(AnomalyEventProcessor.METRIC_ANOMALY_ACTION_EVENT_TYPE)
            .setEventRecordMetadata(Map.of())
            .build();

    ActionEvent actionEvent =
        ActionEvent.newBuilder()
            .setEventRecord(eventRecord)
            .setActionEventMetadata(Map.of())
            .setTenantId("tenant-1")
            .setEventTimeMillis(System.currentTimeMillis())
            .build();

    WebhookNotifier webhookNotifier = Mockito.mock(WebhookNotifier.class);
    new AnomalyEventProcessor(List.of(notificationChannel), webhookNotifier).process(actionEvent);

    Mockito.verify(webhookNotifier).notify(Mockito.any(), Mockito.any());
  }
}
