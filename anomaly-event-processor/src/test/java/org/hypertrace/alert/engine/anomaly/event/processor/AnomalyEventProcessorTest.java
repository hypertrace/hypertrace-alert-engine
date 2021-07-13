package org.hypertrace.alert.engine.anomaly.event.processor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.hypertrace.alert.engine.anomaly.event.processor.NotificationChannel.WebFormatNotificationChannelConfig;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.ActionEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.EventRecord;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyViolation;
import org.junit.jupiter.api.Test;

class AnomalyEventProcessorTest {

  @Test
  public void test1() throws IOException {
    NotificationChannel notificationChannel =
        NotificationChannel.builder()
            .channelName("1")
            .channelId("1")
            .notificationChannelConfig(
                List.of(
                    WebFormatNotificationChannelConfig.builder()
                        .channelConfigType(NotificationChannel.WEBHOOK_CHANNEL_CONFIG_TYPE)
                        .url(
                            "https://hooks.slack.com/services/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")
                        .webhookFormat(NotificationChannel.WEBHOOK_FORMAT_SLACK)
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
            .setEventType("MetricAnomalyViolation")
            .setEventRecordMetadata(Map.of())
            .build();

    ActionEvent actionEvent =
        ActionEvent.newBuilder()
            .setEventRecord(eventRecord)
            .setActionEventMetadata(Map.of())
            .setTenantId("tenant-1")
            .setEventTimeMillis(System.currentTimeMillis())
            .build();

    new AnomalyEventProcessor(List.of(notificationChannel)).process(actionEvent);
  }
}
