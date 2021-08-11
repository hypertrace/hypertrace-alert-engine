package org.hypertrace.alert.engine.metric.anomaly.detector.evaluator;

import static org.hypertrace.alert.engine.metric.anomaly.detector.MetricAnomalyDetectorConstants.TENANT_ID_KEY;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAnomalyEventCondition;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.StaticThresholdCondition;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticRuleEvaluator {

  private static final Logger LOGGER = LoggerFactory.getLogger(StaticRuleEvaluator.class);

  private final QueryRequestHandler queryRequestHandler;

  public StaticRuleEvaluator(QueryRequestHandler queryRequestHandler) {
    this.queryRequestHandler = queryRequestHandler;
  }

  EvaluationResult evaluateRule(
      MetricAnomalyEventCondition metricAnomalyEventCondition, AlertTask alertTask) {

    QueryRequest queryRequest =
        queryRequestHandler.getQueryRequest(metricAnomalyEventCondition, alertTask);
    LOGGER.debug("Query request {}", queryRequest);

    Iterator<ResultSetChunk> iterator =
        queryRequestHandler.executeQuery(
            Map.of(TENANT_ID_KEY, alertTask.getTenantId()), queryRequest);

    StaticThresholdCondition staticThresholdCondition =
        metricAnomalyEventCondition
            .getViolationConditionList()
            .get(0)
            .getStaticThresholdCondition();

    int dataCount = 0, violationCount = 0;
    List<Double> metricValues = new ArrayList<>();

    while (iterator.hasNext()) {
      ResultSetChunk resultSetChunk = iterator.next();
      for (Row row : resultSetChunk.getRowList()) {
        Value value = row.getColumn(1);
        if (value.getValueType() != ValueType.STRING) {
          throw new IllegalArgumentException(
              "Expecting value of type string, received valueType: " + value.getValueType());
        }
        dataCount++;
        LOGGER.debug("Metric data {}", value.getString());
        if (compareThreshold(value, staticThresholdCondition)) {
          violationCount++;
        }
        metricValues.add(value.getDouble());
      }
    }

    if (isViolation(dataCount, violationCount)) {
      LOGGER.debug("Rule violated. dataCount {}, violationCount {}", dataCount, violationCount);
    } else {
      LOGGER.debug("Rule normal. dataCount {} violationCount {}", dataCount, violationCount);
    }

    return EvaluationResult.builder()
        .dataCount(dataCount)
        .violationCount(violationCount)
        .isViolation(isViolation(dataCount, violationCount))
        .operator(
            staticThresholdOperatorToOperatorConvertor(staticThresholdCondition.getOperator()))
        .rhs(getConditionRhs(staticThresholdCondition))
        .metricValues(metricValues)
        .build();
  }

  boolean compareThreshold(Value value, StaticThresholdCondition staticThresholdCondition) {
    double lhs = Double.parseDouble(value.getString());
    double rhs = staticThresholdCondition.getValue();
    return evalOperator(staticThresholdCondition.getOperator(), lhs, rhs);
  }

  private double getConditionRhs(StaticThresholdCondition staticThresholdCondition) {
    return staticThresholdCondition.getValue();
  }

  private boolean isViolation(int dataCount, int violationCount) {
    return dataCount > 0 && (dataCount == violationCount);
  }

  private boolean evalOperator(
      org.hypertrace.alert.engine.eventcondition.config.service.v1.StaticThresholdOperator operator,
      double lhs,
      double rhs) {
    switch (operator) {
      case STATIC_THRESHOLD_OPERATOR_GT:
        return lhs > rhs;
      case STATIC_THRESHOLD_OPERATOR_LT:
        return lhs < rhs;
      case STATIC_THRESHOLD_OPERATOR_GTE:
        return lhs >= rhs;
      case STATIC_THRESHOLD_OPERATOR_LTE:
        return lhs <= rhs;
      default:
        throw new UnsupportedOperationException(
            "Unsupported threshold condition operator: " + operator);
    }
  }

  private Operator staticThresholdOperatorToOperatorConvertor(
      org.hypertrace.alert.engine.eventcondition.config.service.v1.StaticThresholdOperator
          operator) {
    switch (operator) {
      case STATIC_THRESHOLD_OPERATOR_GT:
        return Operator.STATIC_THRESHOLD_OPERATOR_GT;
      case STATIC_THRESHOLD_OPERATOR_LT:
        return Operator.STATIC_THRESHOLD_OPERATOR_LT;
      case STATIC_THRESHOLD_OPERATOR_GTE:
        return Operator.STATIC_THRESHOLD_OPERATOR_GTE;
      case STATIC_THRESHOLD_OPERATOR_LTE:
        return Operator.STATIC_THRESHOLD_OPERATOR_LTE;
      default:
        throw new UnsupportedOperationException(
            "Unsupported threshold condition operator: " + operator);
    }
  }
}
