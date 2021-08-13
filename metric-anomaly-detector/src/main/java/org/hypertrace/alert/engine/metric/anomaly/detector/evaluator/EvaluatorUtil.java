package org.hypertrace.alert.engine.metric.anomaly.detector.evaluator;

public class EvaluatorUtil {
  static boolean isViolation(int dataCount, int violationCount) {
    return dataCount > 0 && (dataCount == violationCount);
  }
}
