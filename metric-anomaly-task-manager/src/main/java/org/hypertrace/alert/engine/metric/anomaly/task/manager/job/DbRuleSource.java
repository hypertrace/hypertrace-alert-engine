package org.hypertrace.alert.engine.metric.anomaly.task.manager.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.rule.source.RuleSource;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.spi.PlatformServiceLifecycle;

public class DbRuleSource implements RuleSource {

  private static final String EVENT_CONDITION_MUTABLE_DATA_KEY = "eventConditionMutableData";
  private static final String METRIC_ANOMALY_DATA_KEY = "metricAnomalyEventCondition";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final String resourceNamespace;
  private final String resourceName;
  private final List<String> tenantIds;

  public DbRuleSource(Config dataStoreConfig, PlatformServiceLifecycle lifecycle) {
    ManagedChannel configChannel =
        ManagedChannelBuilder.forAddress(
                dataStoreConfig.getString("config.service.host"),
                dataStoreConfig.getInt("config.service.port"))
            .usePlaintext()
            .build();
    lifecycle.shutdownComplete().thenRun(configChannel::shutdown);
    this.resourceName = dataStoreConfig.getString("resourceName");
    this.resourceNamespace = dataStoreConfig.getString("resourceNamespace");
    configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    this.tenantIds = dataStoreConfig.getStringList("tenantIds");
  }

  @Override
  public List<Document> getAllRules(Predicate<JsonNode> predicate) {
    return tenantIds.stream()
        .flatMap(tenantId -> getForTenant(tenantId).stream())
        .collect(Collectors.toUnmodifiableList());
  }

  private List<Document> getForTenant(String tenantId) {
    RequestContext context = RequestContext.forTenantId(tenantId);
    return context
        .call(
            () ->
                this.configServiceBlockingStub.getAllConfigs(
                    GetAllConfigsRequest.newBuilder()
                        .setResourceName(this.resourceName)
                        .setResourceNamespace(this.resourceNamespace)
                        .build()))
        .getContextSpecificConfigsList()
        .stream()
        .map(ContextSpecificConfig::getConfig)
        .map(this::convert)
        .collect(Collectors.toUnmodifiableList());
  }

  private Document convert(Value value) {
    JsonNode jsonNode = null;
    try {
      jsonNode = OBJECT_MAPPER.readTree(JsonFormat.printer().print(value));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return new JSONDocument(
        jsonNode.get(EVENT_CONDITION_MUTABLE_DATA_KEY).get(METRIC_ANOMALY_DATA_KEY));
  }
}
