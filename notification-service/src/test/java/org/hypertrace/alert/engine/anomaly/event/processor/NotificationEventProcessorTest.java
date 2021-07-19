package org.hypertrace.alert.engine.anomaly.event.processor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.hypertrace.alert.notification.service.NotificationChannel;
import org.hypertrace.alert.notification.service.NotificationChannel.WebFormatNotificationChannelConfig;
import org.hypertrace.alert.notification.service.NotificationChannelsReader;
import org.hypertrace.alert.notification.service.NotificationEventProcessor;
import org.hypertrace.alert.notification.service.notification.WebhookNotifier;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.EventRecord;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyNotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.NotificationEvent;
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
                        .url("https://hooks.slack.com/services/abcd")
                        .webhookFormat(NotificationChannelsReader.WEBHOOK_FORMAT_SLACK)
                        .build()))
            .build();

    MetricAnomalyNotificationEvent metricAnomalyNotificationEvent =
        MetricAnomalyNotificationEvent.newBuilder()
            .setChannelId("1")
            .setEventConditionId("5")
            .setViolationTimestamp(System.currentTimeMillis())
            .setEventConditionType("grth")
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
            .setActionEventMetadata(Map.of())
            .setTenantId("tenant-1")
            .setEventTimeMillis(System.currentTimeMillis())
            .build();

    WebhookNotifier webhookNotifier = Mockito.mock(WebhookNotifier.class);
    new NotificationEventProcessor(List.of(notificationChannel), webhookNotifier)
        .process(notificationEvent);

    Mockito.verify(webhookNotifier).notify(Mockito.any(), Mockito.any());
  }
}
