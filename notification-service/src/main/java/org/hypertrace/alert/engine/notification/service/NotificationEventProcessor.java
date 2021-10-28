package org.hypertrace.alert.engine.notification.service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.MetricAnomalyNotificationEvent;
import org.hypertrace.alert.engine.notification.service.notification.WebhookNotifier;
import org.hypertrace.alert.engine.notification.transport.webhook.WebhookSender;
import org.hypertrace.alert.engine.notification.transport.webhook.http.HttpWithJsonSender;
import org.hypertrace.core.serviceframework.spi.PlatformServiceLifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationEventProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(NotificationEventProcessor.class);

  static final String METRIC_ANOMALY_ACTION_EVENT_TYPE = "MetricAnomalyViolation";
  private static final int CACHE_MAX_SIZE = 1000;
  private static final int WRITE_EXPIRY_MINUTES = 10;
  private static final int REFRESH_EXPIRY_MINUTES = 10;

  private static final HttpWithJsonSender HTTP_WITH_JSON_SENDER = HttpWithJsonSender.getInstance();
  private final WebhookNotifier webhookNotifier;
  private final NotificationChannelsReader notificationChannelsReader;
  private final LoadingCache<String, Map<String, NotificationChannel>> notificationChannelsCache;

  public NotificationEventProcessor(
      Config ruleSourceConfig, PlatformServiceLifecycle platformServiceLifecycle) {
    this.webhookNotifier = new WebhookNotifier(new WebhookSender(HTTP_WITH_JSON_SENDER));
    notificationChannelsReader =
        new NotificationChannelsReader(ruleSourceConfig, platformServiceLifecycle);
    notificationChannelsCache =
        CacheBuilder.newBuilder()
            .maximumSize(CACHE_MAX_SIZE)
            .expireAfterWrite(WRITE_EXPIRY_MINUTES, TimeUnit.MINUTES)
            .refreshAfterWrite(REFRESH_EXPIRY_MINUTES, TimeUnit.MINUTES)
            .build(
                CacheLoader.asyncReloading(
                    CacheLoader.from(this::loadValue), Executors.newSingleThreadExecutor()));
  }

  private Map<String, NotificationChannel> loadValue(String key) {
    return getNotificationChannelsMap(
        notificationChannelsReader.readAllNotificationChannelsForTenant(key));
  }

  private Map<String, NotificationChannel> getNotificationChannelsMap(
      List<NotificationChannel> notificationChannelsList) {
    Map<String, NotificationChannel> notificationChannelMap = new HashMap<>();
    for (NotificationChannel notificationChannel : notificationChannelsList) {
      notificationChannelMap.put(notificationChannel.getChannelId(), notificationChannel);
    }
    return notificationChannelMap;
  }

  public void process(MetricAnomalyNotificationEvent notificationEvent) {

    LOGGER.debug("Processing notification {}", notificationEvent);

    NotificationChannel notificationChannel =
        getNotificationChannel(
            notificationEvent.getTenantId(), notificationEvent.getChannelId());
    if (notificationChannel != null) {
      LOGGER.debug("Sending notification event {}", notificationEvent);
      webhookNotifier.notify(notificationEvent, notificationChannel);
    }
  }

  public NotificationChannel getNotificationChannel(String tenantId, String channelId) {
    try {
      return notificationChannelsCache.get(tenantId).get(channelId);
    } catch (ExecutionException e) {
      LOGGER.error(
          String.format(
              "Error loading notification channel mutable data for channelId %s tenantId %s",
              channelId, tenantId),
          e);
      return null;
    }
  }
}
