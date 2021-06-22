package org.hypertrace.alert.engine.metric.anomaly.detector;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import org.hypertrace.alert.engine.metric.anomaly.rule.config.service.v1.MetricAggregationFunctionType;
import org.hypertrace.alert.engine.metric.anomaly.rule.config.service.v1.MetricSelection;
import org.hypertrace.alert.engine.metric.anomaly.rule.config.service.v1.Period;
import org.hypertrace.core.attribute.service.client.AttributeServiceClient;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeMetadataFilter;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
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

  QueryRequest buildMetricQueryRequest(MetricSelection anomalyRuleMetricSelection) {
    QueryRequest.Builder builder = QueryRequest.newBuilder();
    builder.setFilter(convertFilter(anomalyRuleMetricSelection.getFilters()));
    setSelection(
        builder,
        anomalyRuleMetricSelection.getKey(),
        anomalyRuleMetricSelection.getScope(),
        anomalyRuleMetricSelection.getAggFunctionType());
    String timeColumn =
        getTimestampAttributeMetadata(anomalyRuleMetricSelection.getScope()).getId();
    builder.addGroupBy(
        createTimeColumnGroupByExpression(
            timeColumn,
            convertPeriodToSeconds(anomalyRuleMetricSelection.getAggInterval().getPeriod())));
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

  static Filter convertFilter(
      org.hypertrace.alert.engine.metric.anomaly.rule.config.service.v1.Filter anomalyRuleFilter) {
    if (anomalyRuleFilter.equals(
        org.hypertrace.alert.engine.metric.anomaly.rule.config.service.v1.Filter
            .getDefaultInstance())) {
      return Filter.getDefaultInstance();
    }
    Filter.Builder builder = Filter.newBuilder();
    builder.setOperator(convertOperator(anomalyRuleFilter.getOperator()));
    if (anomalyRuleFilter.getChildFilterCount() > 0) {
      // if there are child filters, handle them recursively.
      for (org.hypertrace.alert.engine.metric.anomaly.rule.config.service.v1.Filter child :
          anomalyRuleFilter.getChildFilterList()) {
        builder.addChildFilter(convertFilter(child));
      }
    } else {
      // Copy the lhs and rhs from the filter.
      builder.setLhs(convertToQueryExpression(anomalyRuleFilter.getLhs()));
      builder.setRhs(convertToQueryExpression(anomalyRuleFilter.getRhs()));
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

  static Operator convertOperator(
      org.hypertrace.alert.engine.metric.anomaly.rule.config.service.v1.Operator operator) {
    switch (operator) {
      case OPERATOR_EQ:
        return Operator.EQ;
      default:
        throw new IllegalArgumentException("Unsupported operator " + operator.name());
    }
  }
}
