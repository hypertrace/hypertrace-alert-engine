package org.hypertrace.alert.engine.anomaly.event.processor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hypertrace.alert.engine.anomaly.event.processor.notification.WebhookNotifier;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.ActionEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.EventRecord;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyViolation;
import org.hypertrace.alert.engine.notification.transport.webhook.WebhookSender;
import org.hypertrace.alert.engine.notification.transport.webhook.http.HttpWithJsonSender;

class AnomalyEventProcessor {

  static final String METRIC_ANOMALY_ACTION_EVENT_TYPE = "MetricAnomalyViolation";

  private static final HttpWithJsonSender HTTP_WITH_JSON_SENDER = HttpWithJsonSender.getInstance();

  private final Map<String, NotificationChannel> notificationChannelMap;
  private final WebhookNotifier webhookNotifier;

  AnomalyEventProcessor(List<NotificationChannel> notificationChannels) {
    this.notificationChannelMap = getNotificationChannelMap(notificationChannels);
    this.webhookNotifier = new WebhookNotifier(new WebhookSender(HTTP_WITH_JSON_SENDER));
  }

  AnomalyEventProcessor(
      List<NotificationChannel> notificationChannels, WebhookNotifier webhookNotifier) {
    this.notificationChannelMap = getNotificationChannelMap(notificationChannels);
    this.webhookNotifier = webhookNotifier;
  }

  private Map<String, NotificationChannel> getNotificationChannelMap(
      List<NotificationChannel> notificationChannels) {
    return notificationChannels.stream()
        .collect(Collectors.toMap(NotificationChannel::getChannelId, Function.identity()));
  }

  void process(ActionEvent actionEvent) throws IOException {
    EventRecord eventRecord = actionEvent.getEventRecord();
    if (!eventRecord.getEventType().equals(METRIC_ANOMALY_ACTION_EVENT_TYPE)) {
      return;
    }
    MetricAnomalyViolation metricAnomalyViolation =
        MetricAnomalyViolation.fromByteBuffer(eventRecord.getEventValue());
    if (notificationChannelMap.containsKey(metricAnomalyViolation.getChannelId())) {
      webhookNotifier.notify(
          metricAnomalyViolation,
          notificationChannelMap.get(metricAnomalyViolation.getChannelId()));
    }
  }
}
