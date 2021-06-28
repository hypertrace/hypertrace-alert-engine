package org.hypertrace.alert.engine.metric.anomaly.detector;

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

  public static final String DATE_TIME_CONVERTER = "dateTimeConvert";
  private final AttributeServiceClient attributesServiceClient;

  public MetricQueryBuilder(AttributeServiceClient attributesServiceClient) {
    this.attributesServiceClient = attributesServiceClient;
  }

  QueryRequest buildMetricQueryRequest(MetricSelection metricSelection, long startTime, long endTime) {
    QueryRequest.Builder builder = QueryRequest.newBuilder();
    String timeColumn =
        getTimestampAttributeMetadata(metricSelection.getMetricAttribute().getScope()).getId();

    org.hypertrace.core.query.service.api.Filter filter = convertFilter(metricSelection.getFilter());
    builder.setFilter(addTimeFilter(filter, startTime, endTime, timeColumn));

    setSelection(builder, metricSelection.getMetricAttribute(), metricSelection.getMetricAggregationFunction());

    builder.addGroupBy(
        createTimeColumnGroupByExpression(
            timeColumn,
            // todo convertPeriodToSeconds(metricSelection.getMetricAggregationInterval())
            15));
    builder.setLimit(QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT);

    return builder.build();
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
    org.hypertrace.core.query.service.api.Filter.Builder builder = org.hypertrace.core.query.service.api.Filter.newBuilder();
    builder.setLhs(convertFilterLhsExpression(filter.getLhsExpression()));
    builder.setRhs(convertFilterRhsExpression(filter.getRhsExpression()));
    builder.setOperator(convertFilterValueOperator(filter.getValueOperator()));
    return builder.build();
  }

  static org.hypertrace.core.query.service.api.Filter convertCompositeFilter(CompositeFilter compositeFilter) {
    org.hypertrace.core.query.service.api.Filter.Builder builder = org.hypertrace.core.query.service.api.Filter.newBuilder()
        .setOperator(convertFilterLogicalOperator(compositeFilter.getLogicalOperator()));

    compositeFilter.getChildFiltersList().forEach(filter -> builder.addChildFilter(convertFilter(filter)));

    return builder.build();
  }

  static Expression.Builder convertFilterLhsExpression(LhsExpression lhsExpression) {
    Expression.Builder builder = Expression.newBuilder();
    switch (lhsExpression.getValueCase()) {
      case ATTRIBUTE:
        return builder.setColumnIdentifier(
            ColumnIdentifier.newBuilder()
                .setColumnName(lhsExpression.getAttribute().getKey())
                .build());
      default:
        throw new IllegalArgumentException("error");
    }
  }

  static Expression.Builder convertFilterRhsExpression(RhsExpression rhsExpression) {
    Expression.Builder builder = Expression.newBuilder();
    switch (rhsExpression.getValueCase()) {
      case STRING_VALUE:
        builder.setLiteral(
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
      org.hypertrace.core.query.service.api.Filter existingFilter, long startTime, long endTime, String timeStampAttributeId) {
    org.hypertrace.core.query.service.api.Filter.Builder timeFilterBuilder = org.hypertrace.core.query.service.api.Filter.newBuilder()
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

  public static org.hypertrace.core.query.service.api.Filter createLongFilter(String columnName, Operator op, long value) {
    return org.hypertrace.core.query.service.api.Filter.newBuilder().setLhs(createColumnExpression(columnName)).setOperator(op).setRhs(createLongLiteralExpression(value)).build();
  }

  public static Expression createLongLiteralExpression(long value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.LONG).setLong(value)))
        .build();
  }

  public static org.hypertrace.core.query.service.api.Filter createFilter(Expression columnExpression, Operator op, Expression value) {
    return org.hypertrace.core.query.service.api.Filter.newBuilder().setLhs(columnExpression).setOperator(op).setRhs(value).build();
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
                            .setColumnName(metricAttribute.getScope() + "." + metricAttribute.getKey())
                            .build()))
            .build();
    queryBuilder.addSelection(Expression.newBuilder().setFunction(function));
  }

  static String convertToQueryRequestFunctionName(MetricAggregationFunction metricAggregationFunction) {
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

  AttributeMetadata getTimestampAttributeMetadata(String attributeScope) {
    Iterator<AttributeMetadata> attributeMetadataIterator =
        attributesServiceClient.findAttributes(
            Map.of(), AttributeMetadataFilter.newBuilder().addScopeString(attributeScope).build());

    while (attributeMetadataIterator.hasNext()) {
      AttributeMetadata metadata = attributeMetadataIterator.next();
      if (metadata.getKey().equals(attributeScope)) {
        return metadata;
      }
    }

    return null;
  }
  /**

  QueryRequest buildMetricQueryRequest(MetricSelection metricSelection) {
    QueryRequest.Builder builder = QueryRequest.newBuilder();
    builder.setFilter(convertFilter(metricSelection.getFilter()));
    setSelection(
        builder,
        metricSelection.getKey(),
        metricSelection.getScope(),
        metricSelection.getAggFunctionType());
    String timeColumn =
        getTimestampAttributeMetadata(metricSelection.getScope()).getId();
    builder.addGroupBy(
        createTimeColumnGroupByExpression(
            timeColumn,
            convertPeriodToSeconds(metricSelection.getAggInterval().getPeriod())));
    builder.setLimit(QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT);

    return builder.build();
  }

  AttributeMetadata getTimestampAttributeMetadata(String attributeScope) {
    Iterator<AttributeMetadata> attributeMetadataIterator =
        attributesServiceClient.findAttributes(
            Map.of(), AttributeMetadataFilter.newBuilder().addScopeString(attributeScope).build());

    while (attributeMetadataIterator.hasNext()) {
      AttributeMetadata metadata = attributeMetadataIterator.next();
      if (metadata.getKey().equals(attributeScope)) {
        return metadata;
      }
    }

    return null;
  }

  static void setSelection(
      QueryRequest.Builder queryBuilder,
      String attributeKey,
      String scope,
      MetricAggregationFunctionType aggregationFunctionType) {
    Function function =
        Function.newBuilder()
            .setFunctionName(convertToQueryRequestFunctionName(aggregationFunctionType))
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder()
                            .setColumnName(scope + "." + attributeKey)
                            .build()))
            .build();
    queryBuilder.addSelection(Expression.newBuilder().setFunction(function));
  }

  static String convertToQueryRequestFunctionName(MetricAggregationFunctionType functionType) {
    switch (functionType) {
      case METRIC_AGGREGATION_FUNCTION_TYPE_SUM:
        return FunctionType.SUM.name();
      case METRIC_AGGREGATION_FUNCTION_TYPE_AVG:
        return FunctionType.AVG.name();
      default:
        throw new IllegalStateException();
    }
  }

  static long convertPeriodToSeconds(Period period) {
    String unit = period.getUnit();
    if (unit.equals("sec")) {
      return period.getValue();
    } else if (unit.equals("min")) {
      return Duration.ofMinutes(period.getValue()).toSeconds();
    } else {
      // consider hrs
      return Duration.ofHours(period.getValue()).toSeconds();
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

  static org.hypertrace.core.query.service.api.Filter convertFilter(Filter filter) {
    if (filter.equals(Filter.getDefaultInstance())) {
      return org.hypertrace.core.query.service.api.Filter.getDefaultInstance();
    }
    org.hypertrace.core.query.service.api.Filter.Builder builder = org.hypertrace.core.query.service.api.Filter.newBuilder();
    if (filter.hasLeafFilter()) {

    }
      builder.setOperator(convertOperator(filter.getOperator()));
    if (filter.getChildFilterCount() > 0) {
      // if there are child filters, handle them recursively.
      for (org.hypertrace.alert.engine.metric.anomaly.rule.config.service.v1.Filter child :
          filter.getChildFilterList()) {
        builder.addChildFilter(convertFilter(child));
      }
    } else {
      // Copy the lhs and rhs from the filter.
      builder.setLhs(convertToQueryExpression(filter.getLhs()));
      builder.setRhs(convertToQueryExpression(filter.getRhs()));
    }

    return builder.build();
  }

  static Expression.Builder convertToQueryExpression(
      org.hypertrace.alert.engine.metric.anomaly.rule.config.service.v1.Expression expression) {
    Expression.Builder builder = Expression.newBuilder();
    switch (expression.getValueCase()) {
      case VALUE_NOT_SET:
        break;
      case ATTRIBUTE:
        builder.setColumnIdentifier(
            ColumnIdentifier.newBuilder()
                .setColumnName(expression.getAttribute().getKey())
                .build());
        break;
      case LITERAL:
        builder.setLiteral(
            LiteralConstant.newBuilder()
                .setValue(convertValue(expression.getLiteral().getValue()))
                .build());
        break;
    }

    return builder;
  }

  static Value convertValue(
      org.hypertrace.alert.engine.metric.anomaly.rule.config.service.v1.Value value) {
    Value.Builder builder = Value.newBuilder();

    switch (value.getValueType()) {
      case VALUE_TYPE_STRING:
        builder.setValueType(ValueType.STRING);
        builder.setString(value.getString());
        break;
      default:
        throw new IllegalArgumentException("");
    }

    return builder.build();
  }

  static Operator convertOperator(Operator operator) {
    switch (operator) {
      case OPERATOR_EQ:
        return Operator.EQ;
      default:
        throw new IllegalArgumentException("Unsupported operator " + operator.name());
    }
  }
**/
}