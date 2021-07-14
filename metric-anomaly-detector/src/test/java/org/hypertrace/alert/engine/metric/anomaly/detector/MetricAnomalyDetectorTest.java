package org.hypertrace.alert.engine.metric.anomaly.detector;

import static org.mockito.ArgumentMatchers.any;
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
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.ResultSetMetadata;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

class MetricAnomalyDetectorTest {

  @Test
  @Disabled
  void testRuleEvaluation() throws URISyntaxException, MalformedURLException {

    LhsExpression lhsExpression =
        LhsExpression.newBuilder()
            .setAttribute(Attribute.newBuilder().setKey("name").setScope("SERVICE").build())
            .build();
    RhsExpression rhsExpression = RhsExpression.newBuilder().setStringValue("customer").build();
    LeafFilter leafFilter =
        LeafFilter.newBuilder()
            .setValueOperator(ValueOperator.VALUE_OPERATOR_EQ)
            .setLhsExpression(lhsExpression)
            .setRhsExpression(rhsExpression)
            .build();

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

    Config config =
        ConfigFactory.parseURL(
            Thread.currentThread()
                .getContextClassLoader()
                .getResource("application.conf")
                .toURI()
                .toURL());

    MetricAnomalyDetector metricAnomalyDetector = new MetricAnomalyDetector(config);

    /**
     * Query that's hitting pinot
     *
     * <p>Select
     * dateTimeConvert(start_time_millis,'1:MILLISECONDS:EPOCH','1:MILLISECONDS:EPOCH','15:SECONDS'),
     * SUM(duration_millis) FROM rawServiceView WHERE tenant_id = 'default' AND ( (
     * start_time_millis >= ? AND start_time_millis < ? ) AND service_name = 'customer' ) GROUP BY
     * dateTimeConvert(start_time_millis,'1:MILLISECONDS:EPOCH','1:MILLISECONDS:EPOCH','15:SECONDS')
     * limit 10000
     */
    metricAnomalyDetector.process(alertTaskBuilder.build());
  }

  @Test
  void testMetricAnomaly() throws URISyntaxException, MalformedURLException {

    // create mock alertTask
    LhsExpression lhsExpression =
        LhsExpression.newBuilder()
            .setAttribute(Attribute.newBuilder().setKey("name").setScope("SERVICE").build())
            .build();

    RhsExpression rhsExpression = RhsExpression.newBuilder().setStringValue("customer").build();

    LeafFilter leafFilter =
        LeafFilter.newBuilder()
            .setValueOperator(ValueOperator.VALUE_OPERATOR_EQ)
            .setLhsExpression(lhsExpression)
            .setRhsExpression(rhsExpression)
            .build();

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
    AttributeServiceClient attributesServiceClient = mock(AttributeServiceClient.class);
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
    when(attributesServiceClient.findAttributes(
            eq(Map.of("x-tenant-id", "__default")),
            eq(
                AttributeMetadataFilter.newBuilder()
                    .addScopeString(AttributeScope.SERVICE.name())
                    .build())))
        .thenAnswer((Answer<Iterator<AttributeMetadata>>) invocation -> attributesList1.iterator());

    // create mock queryRequest
    long endTime = System.currentTimeMillis();
    long startTime = endTime - 1000;
    org.hypertrace.core.query.service.api.Filter queryServiceFilter =
        createQsDefaultRequestFilter("API.startTime", "API.apiId", startTime, endTime);
    QueryRequest expectedQueryRequest =
        QueryRequest.newBuilder()
            .addSelection(
                MetricQueryBuilder.createColumnExpression(
                    "API.apiId")) // Added implicitly in the getEntitiesAndAggregatedMetrics() in
            // order to do GroupBy on the entity id
            .addSelection(createColumnExpression("API.apiName", "API Name"))
            // QueryServiceEntityFetcher adds Count(entityId) to the request for one that does not
            // have an aggregation.
            // This is because internally a GroupBy request is created out of the entities request
            // and
            // an aggregation is needed.
            .addSelection(createQsAggregationExpression("COUNT", "API.apiId"))
            .setFilter(queryServiceFilter)
            .addGroupBy(MetricQueryBuilder.createColumnExpression("API.apiId"))
            .addGroupBy(createColumnExpression("API.apiName", "API Name"))
            .setLimit(QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT)
            .build();

    // create mock queryServiceClient
    QueryServiceClient queryServiceClient = Mockito.mock(QueryServiceClient.class);
    when(queryServiceClient.executeQuery(eq(expectedQueryRequest), any(), Mockito.anyInt()))
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

    MetricAnomalyDetector metricAnomalyDetector =
        new MetricAnomalyDetector(config, attributesServiceClient, queryServiceClient);
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

  public static Expression createColumnExpression(String columnName, String alias) {
    return Expression.newBuilder()
        .setColumnIdentifier(
            ColumnIdentifier.newBuilder().setColumnName(columnName).setAlias(alias))
        .build();
  }

  public static Expression createQsAggregationExpression(String functionName, String columnName) {
    return Expression.newBuilder()
        .setFunction(
            Function.newBuilder()
                .setFunctionName(functionName)
                .addArguments(MetricQueryBuilder.createColumnExpression(columnName)))
        .build();
  }

  public static org.hypertrace.core.query.service.api.Filter createQsDefaultRequestFilter(
      String timestampColumnName, String entityIdColumnName, long startTime, long endTime) {
    return org.hypertrace.core.query.service.api.Filter.newBuilder()
        .setOperator(Operator.AND)
        .addChildFilter(
            createFilter(
                MetricQueryBuilder.createColumnExpression(entityIdColumnName),
                Operator.NEQ,
                createStringNullLiteralExpression()))
        .addAllChildFilter(
            createBetweenTimesFilter(timestampColumnName, startTime, endTime).getChildFilterList())
        .build();
  }

  public static Expression createStringNullLiteralExpression() {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.NULL_STRING)))
        .build();
  }

  public static org.hypertrace.core.query.service.api.Filter createBetweenTimesFilter(
      String columnName, long lower, long higher) {
    return org.hypertrace.core.query.service.api.Filter.newBuilder()
        .setOperator(Operator.AND)
        .addChildFilter(MetricQueryBuilder.createLongFilter(columnName, Operator.GE, lower))
        .addChildFilter(MetricQueryBuilder.createLongFilter(columnName, Operator.LT, higher))
        .build();
  }

  public static org.hypertrace.core.query.service.api.Filter createFilter(
      Expression columnExpression, Operator op, Expression value) {
    return org.hypertrace.core.query.service.api.Filter.newBuilder()
        .setLhs(columnExpression)
        .setOperator(op)
        .setRhs(value)
        .build();
  }
}
