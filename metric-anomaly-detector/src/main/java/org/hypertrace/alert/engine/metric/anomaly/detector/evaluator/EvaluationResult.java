package org.hypertrace.alert.engine.metric.anomaly.detector.evaluator;

import java.util.List;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.Operator;

@SuperBuilder
@Getter
public class EvaluationResult {
  private final int violationCount;
  private final int dataCount;
  private final boolean isViolation;
  private final Operator operator;
  private final double rhs;
  private final List<Double> metricValues;
}
