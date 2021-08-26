package org.hypertrace.alert.engine.metric.anomaly.detector.evaluator;

import static org.hypertrace.alert.engine.metric.anomaly.detector.evaluator.AlertRuleEvaluatorTest.createLeafFilter;
import static org.hypertrace.alert.engine.metric.anomaly.detector.evaluator.AlertRuleEvaluatorTest.createLhsExpression;
import static org.hypertrace.alert.engine.metric.anomaly.detector.evaluator.AlertRuleEvaluatorTest.createRhsExpression;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import org.apache.commons.lang3.tuple.Pair;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.Attribute;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.Filter;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.LeafFilter;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.LhsExpression;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAggregationFunction;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricSelection;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.RhsExpression;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.ValueOperator;
import org.hypertrace.alert.engine.metric.anomaly.detector.evaluator.MetricCache.MetricTimeSeries;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class MetricCacheTest {

  @Test
  void testMetricCache() {
    QueryRequestHandler queryRequestHandler = mock(QueryRequestHandler.class);
    Iterator<ResultSetChunk> itr =
        List.of(
                AlertRuleEvaluatorTest.getResultSetChunk(
                    List.of(
                        new StringJoiner(".")
                            .add(AttributeScope.SERVICE.name())
                            .add("startTime")
                            .toString(),
                        new StringJoiner(".")
                            .add(AttributeScope.SERVICE.name())
                            .add("duration")
                            .toString()),
                    new String[][] {
                      {String.valueOf(Duration.ofMinutes(1).toMillis()), "100"},
                      {String.valueOf(Duration.ofMinutes(2).toMillis()), "100"},
                      {String.valueOf(Duration.ofMinutes(3).toMillis()), "100"},
                      {String.valueOf(Duration.ofMinutes(4).toMillis()), "100"},
                      {String.valueOf(Duration.ofMinutes(5).toMillis()), "100"},
                    }))
            .iterator();

    Mockito.when(
            queryRequestHandler.executeQuery(anyMap(), any(), anyString(), anyLong(), anyLong()))
        .thenReturn(itr);

    MetricCache metricCache = new MetricCache(queryRequestHandler);

    LhsExpression lhsExpression = createLhsExpression("name", AttributeScope.SERVICE.name());
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

    String tenantId = "tenant1";
    long currentTime = Duration.ofMinutes(5).toMillis();
    // loads data from pinot
    List<Pair<Long, Double>> list =
        metricCache.getMetricValues(
            Map.of(),
            metricSelection,
            tenantId,
            currentTime - Duration.ofMinutes(5).toMillis(),
            currentTime);
    Assertions.assertEquals(5, list.size());
    MetricTimeSeries metricTimeSeries =
        metricCache.getMetricTimeSeriesRecord(tenantId, metricSelection);
    Assertions.assertEquals(5, metricTimeSeries.getDataList().size());

    // returns data from cache
    list =
        metricCache.getMetricValues(
            Map.of(),
            metricSelection,
            tenantId,
            currentTime - Duration.ofMinutes(3).toMillis(),
            currentTime);
    Assertions.assertEquals(4, list.size());
    metricTimeSeries = metricCache.getMetricTimeSeriesRecord(tenantId, metricSelection);
    Assertions.assertEquals(5, metricTimeSeries.getDataList().size());

    // triggers trimming of data
    list =
        metricCache.getMetricValues(
            Map.of(),
            metricSelection,
            tenantId,
            currentTime - Duration.ofMinutes(1).toMillis(),
            currentTime + Duration.ofMinutes(2).toMillis());
    Assertions.assertEquals(2, list.size()); // [4,7]
    metricTimeSeries = metricCache.getMetricTimeSeriesRecord(tenantId, metricSelection);
    Assertions.assertEquals(4, metricTimeSeries.getDataList().size()); // [2, 5]
  }
}
