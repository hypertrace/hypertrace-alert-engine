package org.hypertrace.alert.engine.metric.anomaly.detector;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.Map;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.Attribute;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.CompositeFilter;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.Filter;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.LeafFilter;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.LhsExpression;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.LogicalOperator;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAggregationFunction;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricSelection;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.RhsExpression;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.ValueOperator;
import org.hypertrace.core.attribute.service.client.AttributeServiceClient;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeMetadataFilter;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.gateway.service.v1.common.FunctionType;

public class MetricQueryBuilder {

  private static final String START_TIME_ATTRIBUTE_KEY = "startTime";
  private static final String DATE_TIME_CONVERTER = "dateTimeConvert";

  private final AttributeServiceClient attributesServiceClient;

  public MetricQueryBuilder(AttributeServiceClient attributesServiceClient) {
    this.attributesServiceClient = attributesServiceClient;
  }

  QueryRequest buildMetricQueryRequest(
      MetricSelection metricSelection,
      long startTime, long endTime,
      String tenantId) {
    QueryRequest.Builder builder = QueryRequest.newBuilder();
    String timeColumn = getTimestampAttributeId(
        metricSelection.getMetricAttribute().getScope(),
        tenantId);
    if (null == timeColumn) {
      throw new IllegalArgumentException("Error time column is null");
    }
    org.hypertrace.core.query.service.api.Filter filter =
        convertFilter(metricSelection.getFilter());
    builder.setFilter(addTimeFilter(filter, startTime, endTime, timeColumn));

    setSelection(
        builder,
        metricSelection.getMetricAttribute(),
        metricSelection.getMetricAggregationFunction());

    builder.addGroupBy(
        createTimeColumnGroupByExpression(
            timeColumn, isoDurationToSeconds(metricSelection.getMetricAggregationInterval())));
    builder.setLimit(QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT);

    return builder.build();
  }

  static long isoDurationToSeconds(String duration) {
    Duration d = java.time.Duration.parse(duration);
    return d.get(ChronoUnit.SECONDS);
  }

  static org.hypertrace.core.query.service.api.Filter convertFilter(Filter filter) {
    if (filter.equals(Filter.getDefaultInstance())) {
      return org.hypertrace.core.query.service.api.Filter.getDefaultInstance();
    }
    switch (filter.getFilterCase()) {
      case LEAF_FILTER:
        return convertLeafFilter(filter.getLeafFilter());
      case COMPOSITE_FILTER:
        return convertCompositeFilter(filter.getCompositeFilter());
      default:
        return null;
    }
  }

  static org.hypertrace.core.query.service.api.Filter convertLeafFilter(LeafFilter filter) {
    org.hypertrace.core.query.service.api.Filter.Builder builder =
        org.hypertrace.core.query.service.api.Filter.newBuilder();
    builder.setLhs(convertFilterLhsExpression(filter.getLhsExpression()));
    builder.setRhs(convertFilterRhsExpression(filter.getRhsExpression()));
    builder.setOperator(convertFilterValueOperator(filter.getValueOperator()));
    return builder.build();
  }

  static org.hypertrace.core.query.service.api.Filter convertCompositeFilter(
      CompositeFilter compositeFilter) {
    org.hypertrace.core.query.service.api.Filter.Builder builder =
        org.hypertrace.core.query.service.api.Filter.newBuilder()
            .setOperator(convertFilterLogicalOperator(compositeFilter.getLogicalOperator()));

    compositeFilter
        .getChildFiltersList()
        .forEach(filter -> builder.addChildFilter(convertFilter(filter)));

    return builder.build();
  }

  static Expression.Builder convertFilterLhsExpression(LhsExpression lhsExpression) {
    Expression.Builder builder = Expression.newBuilder();
    switch (lhsExpression.getValueCase()) {
      case ATTRIBUTE:
        return builder.setColumnIdentifier(
            ColumnIdentifier.newBuilder()
                .setColumnName(
                    lhsExpression.getAttribute().getScope()
                        + "."
                        + lhsExpression.getAttribute().getKey())
                .build());
      default:
        throw new IllegalArgumentException("error");
    }
  }

  static Expression.Builder convertFilterRhsExpression(RhsExpression rhsExpression) {
    Expression.Builder builder = Expression.newBuilder();
    switch (rhsExpression.getValueCase()) {
      case STRING_VALUE:
        return builder.setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setString(rhsExpression.getStringValue()))
                .build());
      default:
        throw new IllegalArgumentException("error");
    }
  }

  static Operator convertFilterValueOperator(ValueOperator valueOperator) {
    switch (valueOperator) {
      case VALUE_OPERATOR_EQ:
        return Operator.EQ;
      default:
        throw new IllegalArgumentException("error");
    }
  }

  static Operator convertFilterLogicalOperator(LogicalOperator logicalOperator) {
    switch (logicalOperator) {
      case LOGICAL_OPERATOR_OR:
        return Operator.OR;
      case LOGICAL_OPERATOR_AND:
        return Operator.AND;
      default:
        throw new IllegalArgumentException("error");
    }
  }

  static org.hypertrace.core.query.service.api.Filter addTimeFilter(
      org.hypertrace.core.query.service.api.Filter existingFilter,
      long startTime,
      long endTime,
      String timeStampAttributeId) {
    org.hypertrace.core.query.service.api.Filter.Builder timeFilterBuilder =
        org.hypertrace.core.query.service.api.Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(createLongFilter(timeStampAttributeId, Operator.GE, startTime))
            .addChildFilter(createLongFilter(timeStampAttributeId, Operator.LT, endTime));

    if (null == existingFilter) {
      return timeFilterBuilder.build();
    }

    return org.hypertrace.core.query.service.api.Filter.newBuilder()
        .setOperator(Operator.AND)
        .addChildFilter(timeFilterBuilder.build())
        .addChildFilter(existingFilter)
        .build();
  }

  public static org.hypertrace.core.query.service.api.Filter createLongFilter(
      String columnName, Operator op, long value) {
    return org.hypertrace.core.query.service.api.Filter.newBuilder()
        .setLhs(createColumnExpression(columnName))
        .setOperator(op)
        .setRhs(createLongLiteralExpression(value))
        .build();
  }

  public static Expression createLongLiteralExpression(long value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.LONG).setLong(value)))
        .build();
  }

  static void setSelection(
      QueryRequest.Builder queryBuilder,
      Attribute metricAttribute,
      MetricAggregationFunction metricAggregationFunction) {
    Function function =
        Function.newBuilder()
            .setFunctionName(convertToQueryRequestFunctionName(metricAggregationFunction))
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder()
                            .setColumnName(
                                metricAttribute.getScope() + "." + metricAttribute.getKey())
                            .build()))
            .build();
    queryBuilder.addSelection(Expression.newBuilder().setFunction(function));
  }

  static String convertToQueryRequestFunctionName(
      MetricAggregationFunction metricAggregationFunction) {
    switch (metricAggregationFunction) {
      case METRIC_AGGREGATION_FUNCTION_TYPE_SUM:
        return FunctionType.SUM.name();
      case METRIC_AGGREGATION_FUNCTION_TYPE_AVG:
        return FunctionType.AVG.name();
      default:
        throw new IllegalStateException();
    }
  }

  static Expression createTimeColumnGroupByExpression(String timeColumn, long periodSecs) {
    return Expression.newBuilder()
        .setFunction(
            Function.newBuilder()
                .setFunctionName(DATE_TIME_CONVERTER)
                .addArguments(createColumnExpression(timeColumn))
                .addArguments(createStringLiteralExpression("1:MILLISECONDS:EPOCH"))
                .addArguments(createStringLiteralExpression("1:MILLISECONDS:EPOCH"))
                .addArguments(createStringLiteralExpression(periodSecs + ":SECONDS")))
        .build();
  }

  static Expression createColumnExpression(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName))
        .build();
  }

  static Expression createStringLiteralExpression(String value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.STRING).setString(value)))
        .build();
  }

  String getTimestampAttributeId(String tenantId, String attributeScope) {
    Iterator<AttributeMetadata> attributeMetadataIterator =
        attributesServiceClient.findAttributes(
            Map.of(MetricAnomalyDetector.TENANT_ID_KEY, tenantId),
            AttributeMetadataFilter.newBuilder().addScopeString(attributeScope).build());

    while (attributeMetadataIterator.hasNext()) {
      AttributeMetadata metadata = attributeMetadataIterator.next();
      if (metadata.getKey().equals(START_TIME_ATTRIBUTE_KEY)) {
        return metadata.getId();
      }
    }

    return null;
  }
}
