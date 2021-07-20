package org.hypertrace.alert.engine.metric.anomaly.detector;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.Attribute;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.Filter;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.LeafFilter;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.LhsExpression;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAggregationFunction;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAnomalyEventCondition;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricSelection;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.RhsExpression;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.Severity;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.StaticThresholdCondition;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.StaticThresholdOperator;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.ValueOperator;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.ViolationCondition;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.core.attribute.service.client.AttributeServiceClient;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeMetadataFilter;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.ResultSetMetadata;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

class AlertRuleEvaluatorTest {

  static final StringJoiner dotJoiner = new StringJoiner(".");

  @Test
  void testMetricAnomaly() throws URISyntaxException, MalformedURLException {

    // create mock alertTask
    LhsExpression lhsExpression = createLhsExpression("name", "SERVICE");
    RhsExpression rhsExpression = createRhsExpression("customer");
    LeafFilter leafFilter =
        createLeafFilter(ValueOperator.VALUE_OPERATOR_EQ, lhsExpression, rhsExpression);

    MetricSelection metricSelection =
        MetricSelection.newBuilder()
            .setMetricAggregationInterval("PT15s")
            .setMetricAggregationFunction(
                MetricAggregationFunction.METRIC_AGGREGATION_FUNCTION_TYPE_SUM)
            .setFilter(Filter.newBuilder().setLeafFilter(leafFilter).build())
            .setMetricAttribute(
                Attribute.newBuilder().setKey("duration").setScope("SERVICE").build())
            .build();

    MetricAnomalyEventCondition.Builder metricAnomalyEventConditionBuilder =
        MetricAnomalyEventCondition.newBuilder();
    metricAnomalyEventConditionBuilder.setMetricSelection(metricSelection);
    metricAnomalyEventConditionBuilder.addViolationCondition(
        ViolationCondition.newBuilder()
            .setStaticThresholdCondition(
                StaticThresholdCondition.newBuilder()
                    .setOperator(StaticThresholdOperator.STATIC_THRESHOLD_OPERATOR_GT)
                    .setMinimumViolationDuration("PT5M")
                    .setValue(15)
                    .setSeverity(Severity.SEVERITY_CRITICAL)
                    .build())
            .build());

    AlertTask.Builder alertTaskBuilder = AlertTask.newBuilder();
    alertTaskBuilder.setCurrentExecutionTime(System.currentTimeMillis());
    alertTaskBuilder.setLastExecutionTime(
        System.currentTimeMillis() - Duration.ofMinutes(1).toMillis());
    alertTaskBuilder.setEventConditionId("event-condition-1");
    alertTaskBuilder.setEventConditionType("MetricAnomalyEventCondition");
    alertTaskBuilder.setTenantId("__default");
    alertTaskBuilder.setEventConditionValue(
        metricAnomalyEventConditionBuilder.build().toByteString().asReadOnlyByteBuffer());

    // create mock config
    Config config =
        ConfigFactory.parseURL(
            Thread.currentThread()
                .getContextClassLoader()
                .getResource("application.conf")
                .toURI()
                .toURL());

    // create mock attributeServiceClient
    List<AttributeMetadata> attributesList1 =
        List.of(
            AttributeMetadata.newBuilder()
                .setScopeString(AttributeScope.SERVICE.name())
                .setKey("id")
                .setId("Service.id")
                .build(),
            AttributeMetadata.newBuilder()
                .setScopeString(AttributeScope.SERVICE.name())
                .setKey("startTime")
                .setId("Service.startTime")
                .build());

    AttributeServiceClient attributesServiceClient = mock(AttributeServiceClient.class);
    when(attributesServiceClient.findAttributes(
            eq(Map.of("x-tenant-id", "__default")),
            eq(
                AttributeMetadataFilter.newBuilder()
                    .addScopeString(AttributeScope.SERVICE.name())
                    .build())))
        .thenAnswer((Answer<Iterator<AttributeMetadata>>) invocation -> attributesList1.iterator());

    // create mock queryServiceFilter
    org.hypertrace.core.query.service.api.Filter queryServiceFilter =
        org.hypertrace.core.query.service.api.Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(
                org.hypertrace.core.query.service.api.Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addChildFilter(
                        MetricQueryBuilder.createLongFilter(
                            "Service.startTime",
                            Operator.GE,
                            alertTaskBuilder.getLastExecutionTime()))
                    .addChildFilter(
                        MetricQueryBuilder.createLongFilter(
                            "Service.startTime",
                            Operator.LT,
                            alertTaskBuilder.getCurrentExecutionTime())))
            .addChildFilter(MetricQueryBuilder.convertLeafFilter(leafFilter))
            .build();

    // create mock queryRequest
    QueryRequest expectedQueryRequest =
        QueryRequest.newBuilder()
            .addSelection(
                Expression.newBuilder()
                    .setFunction(
                        Function.newBuilder()
                            .setFunctionName(FunctionType.SUM.name())
                            .addArguments(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder()
                                            .setColumnName(
                                                dotJoiner.add("SERVICE").add("duration").toString())
                                            .build()))
                            .build()))
            .setFilter(queryServiceFilter)
            .addGroupBy(
                MetricQueryBuilder.createTimeColumnGroupByExpression(
                    "Service.startTime", MetricQueryBuilder.isoDurationToSeconds("PT15s")))
            .setLimit(QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT)
            .build();

    // create mock queryServiceClient
    QueryServiceClient queryServiceClient = Mockito.mock(QueryServiceClient.class);
    when(queryServiceClient.executeQuery(
            eq(expectedQueryRequest), eq(Map.of("x-tenant-id", "__default")), Mockito.anyInt()))
        .thenReturn(
            List.of(
                    getResultSetChunk(
                        List.of("API.apiId", "API.apiName"),
                        new String[][] {
                          {
                            "apiId1", "/login",
                          },
                          {"apiId2", "/checkout"}
                        }))
                .iterator());

    AlertRuleEvaluator alertRuleEvaluator =
        new AlertRuleEvaluator(config, attributesServiceClient, queryServiceClient);
    //    Assertions.assertTrue(metricAnomalyDetector.process(alertTaskBuilder.build()));
  }

  public static ResultSetChunk getResultSetChunk(
      List<String> columnNames, String[][] resultsTable) {
    ResultSetChunk.Builder resultSetChunkBuilder = ResultSetChunk.newBuilder();

    // ColumnMetadata from the keyset
    List<ColumnMetadata> columnMetadataBuilders =
        columnNames.stream()
            .map(
                (columnName) ->
                    ColumnMetadata.newBuilder()
                        .setColumnName(columnName)
                        .setValueType(ValueType.STRING)
                        .build())
            .collect(Collectors.toList());
    resultSetChunkBuilder.setResultSetMetadata(
        ResultSetMetadata.newBuilder().addAllColumnMetadata(columnMetadataBuilders));

    // Add the rows.
    for (int i = 0; i < resultsTable.length; i++) {
      Row.Builder rowBuilder = Row.newBuilder();
      for (int j = 0; j < resultsTable[i].length; j++) {
        rowBuilder.addColumn(
            Value.newBuilder().setString(resultsTable[i][j]).setValueType(ValueType.STRING));
      }
      resultSetChunkBuilder.addRow(rowBuilder);
    }

    return resultSetChunkBuilder.build();
  }

  static LhsExpression createLhsExpression(String key, String scope) {
    return LhsExpression.newBuilder()
        .setAttribute(Attribute.newBuilder().setKey(key).setScope(scope).build())
        .build();
  }

  static RhsExpression createRhsExpression(String stringValue) {
    return RhsExpression.newBuilder().setStringValue(stringValue).build();
  }

  static LeafFilter createLeafFilter(
      ValueOperator valueOperator, LhsExpression lhsExpression, RhsExpression rhsExpression) {
    return LeafFilter.newBuilder()
        .setValueOperator(valueOperator)
        .setLhsExpression(lhsExpression)
        .setRhsExpression(rhsExpression)
        .build();
  }
}
