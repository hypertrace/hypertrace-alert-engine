package org.hypertrace.alert.engine.notification.service;

import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.spi.PlatformServiceLifecycle;
import org.hypertrace.notification.config.service.v1.GetAllNotificationChannelsRequest;
import org.hypertrace.notification.config.service.v1.NotificationChannel;
import org.hypertrace.notification.config.service.v1.NotificationChannelConfigServiceGrpc;

public class NotificationChannelDbRuleSource {

  private static final String CONFIG_SERVICE_HOST_CONFIG_KEY = "config.service.host";
  private static final String CONFIG_SERVICE_PORT_CONFIG_KEY = "config.service.port";
  private static final String TENANT_IDS_CONFIG_KEY = "tenantIds";

  private final NotificationChannelConfigServiceGrpc.NotificationChannelConfigServiceBlockingStub
      notificationChannelStub;
  private final List<String> tenantIds;

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
    this.tenantIds = dataStoreConfig.getStringList(TENANT_IDS_CONFIG_KEY);
  }

  public List<NotificationChannel> getAllRules() {
    return tenantIds.stream()
        .flatMap(tenantId -> getForTenant(tenantId).stream())
        .collect(Collectors.toUnmodifiableList());
  }

  private List<NotificationChannel> getForTenant(String tenantId) {
    RequestContext context = RequestContext.forTenantId(tenantId);
    return context
        .call(
            () ->
                notificationChannelStub.getAllNotificationChannels(
                    GetAllNotificationChannelsRequest.newBuilder().build()))
        .getNotificationChannelsList();
  }
}
