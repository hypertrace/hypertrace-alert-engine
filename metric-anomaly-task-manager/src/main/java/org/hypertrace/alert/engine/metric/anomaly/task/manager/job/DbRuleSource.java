package org.hypertrace.alert.engine.metric.anomaly.task.manager.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
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
import org.hypertrace.notification.config.service.v1.GetAllNotificationRulesRequest;
import org.hypertrace.notification.config.service.v1.NotificationRule;
import org.hypertrace.notification.config.service.v1.NotificationRuleConfigServiceGrpc;

public class DbRuleSource implements RuleSource {

  private static final String EVENT_CONDITION_DATA_KEY = "eventConditionData";
  private static final String EVENT_CONDITION_MUTABLE_DATA_KEY = "eventConditionMutableData";
  private static final String NOTIFICATION_RULE_MUTABLE_DATA_KEY = "notificationRuleMutableData";
  private static final String METRIC_ANOMALY_DATA_KEY = "metricAnomalyEventCondition";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final NotificationRuleConfigServiceGrpc.NotificationRuleConfigServiceBlockingStub
      notificationRuleStub;
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
    notificationRuleStub =
        NotificationRuleConfigServiceGrpc.newBlockingStub(configChannel)
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
    List<NotificationRule> notificationRules = context.call(
            () -> notificationRuleStub.getAllNotificationRules(
                GetAllNotificationRulesRequest.newBuilder().build()))
        .getNotificationRulesList();
    Map<String, JsonNode> idMetricAnomalyMap = context
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
        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

    return notificationRules.stream()
        .filter(rule -> idMetricAnomalyMap.containsKey(
            rule.getNotificationRuleMutableData().getEventConditionId()))
        .map(rule -> {
          try {
            JsonNode metricAnomalyNode = idMetricAnomalyMap.get(
                rule.getNotificationRuleMutableData().getEventConditionId());
            JsonNode ruleNode = OBJECT_MAPPER.readTree(JsonFormat.printer().print(rule))
                .get(NOTIFICATION_RULE_MUTABLE_DATA_KEY);
            ((ObjectNode)ruleNode).set(AlertTaskConverter.EVENT_CONDITION, metricAnomalyNode);
            return new JSONDocument(ruleNode);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        })
        .collect(Collectors.toList());
  }

  private Pair<String, JsonNode> convert(Value value) {
    JsonNode jsonNode;
    try {
      jsonNode = OBJECT_MAPPER.readTree(JsonFormat.printer().print(value));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    if (jsonNode.get("id") == null) {
      throw new RuntimeException(String.format("id is missing in the object %s", value));
    }
    String id = jsonNode.get("id").textValue();
    if (jsonNode.get(EVENT_CONDITION_DATA_KEY) != null) {
      jsonNode = jsonNode.get(EVENT_CONDITION_DATA_KEY);
    } else if (jsonNode.get(EVENT_CONDITION_MUTABLE_DATA_KEY) != null) {
      jsonNode = jsonNode.get(EVENT_CONDITION_MUTABLE_DATA_KEY);
    } else {
      throw new RuntimeException(String.format("Event condition is missing in the object %s", value));
    }
    if (jsonNode.get(METRIC_ANOMALY_DATA_KEY) == null) {
      throw new RuntimeException(String.format("MetricAnomalyEvent is missing in the object %s", value));
    }
    return Pair.of(id, jsonNode.get(METRIC_ANOMALY_DATA_KEY));
  }
}
