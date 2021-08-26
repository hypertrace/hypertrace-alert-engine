package org.hypertrace.alert.engine.metric.anomaly.detector.evaluator;

import static org.hypertrace.alert.engine.metric.anomaly.detector.MetricQueryBuilder.isoDurationToSeconds;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.Pair;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAggregationFunction;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricSelection;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;

class MetricCache {

  private static final int CACHE_EXPIRY_MINUTES = 5;

  private final Cache<MetricSelection, MetricTimeSeries> metricCache;
  private final QueryRequestHandler queryRequestHandler;

  MetricCache(QueryRequestHandler queryRequestHandler) {
    this.queryRequestHandler = queryRequestHandler;
    this.metricCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(Duration.ofMinutes(CACHE_EXPIRY_MINUTES))
            .build();
  }

  /**
   * Return a list of pair of <timestamp, metric value> <br>
   * Note: Performance optimisation could be achieved by using a variable type container for metric
   * value, thus avoiding cast of integer values to double
   */
  List<Pair<Long, Double>> getMetricValues(
      Map<String, String> requestHeaders,
      MetricSelection metricSelection,
      String tenantId,
      long startTimeMillis,
      long endTimeMillis,
      long alertGapMillis) {
    long metricDurationMillis = endTimeMillis - startTimeMillis;

    MetricTimeSeries metricTimeSeries = metricCache.getIfPresent(metricSelection);
    // no record for this metric selection
    // if requested time range is earlier than existing
    // replace data
    if (null == metricTimeSeries || startTimeMillis < metricTimeSeries.getStartTimeMillis()) {
      Iterator<ResultSetChunk> iterator =
          queryRequestHandler.executeQuery(
              requestHeaders, metricSelection, tenantId, startTimeMillis, endTimeMillis);
      List<Pair<Long, Double>> dataList =
          convertToTimeSeries(iterator, alertGapMillis, metricSelection);
      metricCache.put(
          metricSelection,
          new MetricTimeSeries(startTimeMillis, endTimeMillis, dataList, metricDurationMillis));
      return dataList;
    }

    // need to just get diff of the data
    if (endTimeMillis > metricTimeSeries.getEndTimeMillis()) {
      Iterator<ResultSetChunk> iterator =
          queryRequestHandler.executeQuery(
              requestHeaders,
              metricSelection,
              tenantId,
              metricTimeSeries.getEndTimeMillis(),
              endTimeMillis);
      List<Pair<Long, Double>> dataList =
          convertToTimeSeries(iterator, alertGapMillis, metricSelection);
      metricTimeSeries.getDataList().addAll(dataList);
      metricTimeSeries.setEndTimeMillis(endTimeMillis);
      metricTimeSeries.setMaxRetentionPeriodMillis(
          Math.max(metricTimeSeries.getMaxRetentionPeriodMillis(), metricDurationMillis));

      // trim off any stale data
      metricTimeSeries.trimOlderData();
    }

    return filterData(metricTimeSeries.getDataList(), startTimeMillis, endTimeMillis);
  }

  private List<Pair<Long, Double>> convertToTimeSeries(
      Iterator<ResultSetChunk> iterator, long alertGapMillis, MetricSelection metricSelection) {
    List<Pair<Long, Double>> list = new ArrayList<>();
    while (iterator.hasNext()) {
      ResultSetChunk resultSetChunk = iterator.next();
      for (Row row : resultSetChunk.getRowList()) {
        Value value = row.getColumn(1);
        if (value.getValueType() != ValueType.STRING) {
          throw new IllegalArgumentException(
              "Expecting value of type string, received valueType: " + value.getValueType());
        }

        double doubleValue = Double.parseDouble(value.getString());
        if (metricSelection.getMetricAggregationFunction()
            == MetricAggregationFunction.METRIC_AGGREGATION_FUNCTION_TYPE_AVGRATE) {
          doubleValue =
              getAvgrateValue(
                  alertGapMillis, doubleValue, metricSelection.getMetricAggregationInterval());
        }

        list.add(Pair.of(Long.parseLong(row.getColumn(0).getString()), doubleValue));
      }
    }
    return list;
  }

  private double getAvgrateValue(
      long alertGapMillis, double originalValue, String metricAggregationIntervalPeriod) {
    long periodInSec = isoDurationToSeconds(metricAggregationIntervalPeriod);
    double divisor = (double) alertGapMillis / TimeUnit.SECONDS.toMillis(periodInSec);
    return originalValue / divisor;
  }

  private List<Pair<Long, Double>> filterData(
      List<Pair<Long, Double>> dataList, long startTime, long endTime) {
    return dataList.stream()
        .filter(v -> v.getKey() >= startTime && v.getKey() <= endTime)
        .collect(Collectors.toList());
  }

  MetricTimeSeries getMetricTimeSeriesRecord(MetricSelection metricSelection) {
    return metricCache.getIfPresent(metricSelection);
  }

  @Getter
  @Setter
  static class MetricTimeSeries {

    private long startTimeMillis;
    private long endTimeMillis;
    private List<Pair<Long, Double>> dataList;
    // this is the maximum period across rules for this particular metric
    private long maxRetentionPeriodMillis = -1;

    MetricTimeSeries(
        long startTimeMillis,
        long endTimeMillis,
        List<Pair<Long, Double>> dataList,
        long maxRetentionPeriodMillis) {
      this.startTimeMillis = startTimeMillis;
      this.endTimeMillis = endTimeMillis;
      this.maxRetentionPeriodMillis =
          Math.max(maxRetentionPeriodMillis, this.maxRetentionPeriodMillis);
      this.dataList = dataList;
    }

    void trimOlderData() {
      long updatedStartTime = endTimeMillis - maxRetentionPeriodMillis;
      if (updatedStartTime <= startTimeMillis) {
        return;
      }
      dataList.removeIf(longDoublePair -> longDoublePair.getKey() < updatedStartTime);
      startTimeMillis = updatedStartTime;
    }
  }
}
