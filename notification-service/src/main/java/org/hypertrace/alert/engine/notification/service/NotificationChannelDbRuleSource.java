package org.hypertrace.alert.engine.notification.service;

import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.List;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.spi.PlatformServiceLifecycle;
import org.hypertrace.notification.config.service.v1.GetAllNotificationChannelsRequest;
import org.hypertrace.notification.config.service.v1.NotificationChannel;
import org.hypertrace.notification.config.service.v1.NotificationChannelConfigServiceGrpc;

public class NotificationChannelDbRuleSource {

  private static final String CONFIG_SERVICE_HOST_CONFIG_KEY = "config.service.host";
  private static final String CONFIG_SERVICE_PORT_CONFIG_KEY = "config.service.port";

  private final NotificationChannelConfigServiceGrpc.NotificationChannelConfigServiceBlockingStub
      notificationChannelStub;

  public NotificationChannelDbRuleSource(
      Config dataStoreConfig, PlatformServiceLifecycle lifecycle) {
    ManagedChannel configChannel =
        ManagedChannelBuilder.forAddress(
                dataStoreConfig.getString(CONFIG_SERVICE_HOST_CONFIG_KEY),
                dataStoreConfig.getInt(CONFIG_SERVICE_PORT_CONFIG_KEY))
            .usePlaintext()
            .build();
    lifecycle.shutdownComplete().thenRun(configChannel::shutdown);
    notificationChannelStub =
        NotificationChannelConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  public List<NotificationChannel> getNotificationChannelsForTenant(String tenantId) {
    RequestContext context = RequestContext.forTenantId(tenantId);
    return context
        .call(
            () ->
                notificationChannelStub.getAllNotificationChannels(
                    GetAllNotificationChannelsRequest.newBuilder().build()))
        .getNotificationChannelsList();
  }
}
