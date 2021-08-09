package org.hypertrace.alert.engine.metric.anomaly.detector.evaluator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.StaticThresholdCondition;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.ViolationCondition;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.Operator;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticRuleEvaluator {

  private static final Logger LOGGER = LoggerFactory.getLogger(StaticRuleEvaluator.class);

  static EvaluationResult evaluateForStaticThreshold(
      Iterator<ResultSetChunk> iterator, ViolationCondition violationCondition) {
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
        if (compareThreshold(value, violationCondition)) {
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
            staticThresholdOperatorToOperatorConvertor(
                violationCondition.getStaticThresholdCondition().getOperator()))
        .rhs(getConditionRhs(violationCondition))
        .metricValues(metricValues)
        .build();
  }

  static boolean compareThreshold(Value value, ViolationCondition violationCondition) {
    StaticThresholdCondition thresholdCondition = violationCondition.getStaticThresholdCondition();
    double lhs = Double.parseDouble(value.getString());
    double rhs = thresholdCondition.getValue();
    return evalOperator(thresholdCondition.getOperator(), lhs, rhs);
  }

  private static double getConditionRhs(ViolationCondition violationCondition) {
    return violationCondition.getStaticThresholdCondition().getValue();
  }

  private static boolean isViolation(int dataCount, int violationCount) {
    return dataCount > 0 && (dataCount == violationCount);
  }

  private static boolean evalOperator(
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

  private static Operator staticThresholdOperatorToOperatorConvertor(
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
