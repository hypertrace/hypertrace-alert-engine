package org.hypertrace.alert.engine.notification.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.EventRecord;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyNotificationEvent;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.NotificationEvent;
import org.hypertrace.alert.engine.notification.service.notification.WebhookNotifier;
import org.hypertrace.alert.engine.notification.transport.webhook.WebhookSender;
import org.hypertrace.alert.engine.notification.transport.webhook.http.HttpWithJsonSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationEventProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(NotificationEventProcessor.class);

  static final String METRIC_ANOMALY_ACTION_EVENT_TYPE = "MetricAnomalyViolation";

  private static final HttpWithJsonSender HTTP_WITH_JSON_SENDER = HttpWithJsonSender.getInstance();

  private final Map<String, NotificationChannel> notificationChannelMap;
  private final WebhookNotifier webhookNotifier;

  public NotificationEventProcessor(List<NotificationChannel> notificationChannels) {
    this.notificationChannelMap = getNotificationChannelMap(notificationChannels);
    this.webhookNotifier = new WebhookNotifier(new WebhookSender(HTTP_WITH_JSON_SENDER));
  }

  NotificationEventProcessor(
      List<NotificationChannel> notificationChannels, WebhookNotifier webhookNotifier) {
    this.notificationChannelMap = getNotificationChannelMap(notificationChannels);
    this.webhookNotifier = webhookNotifier;
  }

  private Map<String, NotificationChannel> getNotificationChannelMap(
      List<NotificationChannel> notificationChannels) {
    return notificationChannels.stream()
        .collect(Collectors.toMap(NotificationChannel::getChannelId, Function.identity()));
  }

  public void process(NotificationEvent notificationEvent) {
    EventRecord eventRecord = notificationEvent.getEventRecord();
    if (!eventRecord.getEventType().equals(METRIC_ANOMALY_ACTION_EVENT_TYPE)) {
      LOGGER.debug("Received unsupported event type {}", eventRecord.getEventType());
      return;
    }

    MetricAnomalyNotificationEvent metricAnomalyNotificationEvent;
    try {
      metricAnomalyNotificationEvent = MetricAnomalyNotificationEvent.fromByteBuffer(eventRecord.getEventValue());
    } catch (IOException e) {
      throw new RuntimeException("Exception deserializing MetricAnomalyNotificationEvent", e);
    }
    if (notificationChannelMap.containsKey(metricAnomalyNotificationEvent.getChannelId())) {
      webhookNotifier.notify(
          metricAnomalyNotificationEvent,
          notificationChannelMap.get(metricAnomalyNotificationEvent.getChannelId()));
    }
  }
}
