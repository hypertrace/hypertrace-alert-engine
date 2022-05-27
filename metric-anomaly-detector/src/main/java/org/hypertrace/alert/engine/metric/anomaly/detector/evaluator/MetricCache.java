package org.hypertrace.alert.engine.metric.anomaly.detector.evaluator;

import static org.hypertrace.alert.engine.metric.anomaly.detector.MetricQueryBuilder.isoDurationToSeconds;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.Pair;
import org.hypertrace.alerting.config.service.v1.MetricAggregationFunction;
import org.hypertrace.alerting.config.service.v1.MetricSelection;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

class MetricCache {

  private static final int CACHE_EXPIRY_MINUTES = 5;
  private static final ConcurrentMap<String, AtomicInteger> cacheSizeGauge =
      new ConcurrentHashMap<>();
  private static final String CACHE_SIZE_GAUGE = "hypertrace.metric.anomaly.detector.cache.size";
  private static final ConcurrentMap<String, Timer> longQueryTimer = new ConcurrentHashMap<>();
  private static final String LONG_QUERY_TIMER = "hypertrace.metric.anomaly.detector.query.time";
  // cache key <tenantId, metricSelection>
  private final Cache<Pair<String, MetricSelection>, MetricTimeSeries> metricCache;
  private final QueryRequestHandler queryRequestHandler;

  MetricCache(QueryRequestHandler queryRequestHandler) {
    this.queryRequestHandler = queryRequestHandler;
    this.metricCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(CACHE_EXPIRY_MINUTES, TimeUnit.MINUTES)
            .recordStats()
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
      long endTimeMillis) {
    PlatformMetricsRegistry.registerCache("metricCache", metricCache, Map.of("tenantId", tenantId));
    long metricDurationMillis = endTimeMillis - startTimeMillis;
    Pair<String, MetricSelection> cacheKey = Pair.of(tenantId, metricSelection);
    MetricTimeSeries metricTimeSeries = metricCache.getIfPresent(cacheKey);
    // no record for this metric selection
    // if requested time range is earlier than existing
    // replace data
    if (null == metricTimeSeries || startTimeMillis < metricTimeSeries.getStartTimeMillis()) {
      Instant startTime = Instant.now();
      Iterator<ResultSetChunk> iterator =
          queryRequestHandler.executeQuery(
              requestHeaders, metricSelection, tenantId, startTimeMillis, endTimeMillis);

      // record time if longer query is fired
      if (metricTimeSeries != null && startTimeMillis < metricTimeSeries.getStartTimeMillis()) {
        recordQueryTime(startTime, tenantId);
      }

      List<Pair<Long, Double>> dataList = convertToTimeSeries(iterator, metricSelection);
      metricCache.put(
          cacheKey,
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
      List<Pair<Long, Double>> dataList = convertToTimeSeries(iterator, metricSelection);
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
      Iterator<ResultSetChunk> iterator, MetricSelection metricSelection) {
    List<Pair<Long, Double>> list = new ArrayList<>();
    while (iterator.hasNext()) {
      ResultSetChunk resultSetChunk = iterator.next();
      for (Row row : resultSetChunk.getRowList()) {
        Value value = row.getColumn(1);
        if (value.getValueType() != ValueType.STRING) {
          throw new IllegalArgumentException(
              "Expecting value of type string, received valueType: " + value.getValueType());
        }
        list.add(
            Pair.of(
                Long.parseLong(row.getColumn(0).getString()),
                getDoubleValue(value, metricSelection)));
      }
    }
    return list;
  }

  static double getDoubleValue(Value value, MetricSelection metricSelection) {
    double doubleValue = Double.parseDouble(value.getString());
    if (metricSelection.getMetricAggregationFunction()
        == MetricAggregationFunction.METRIC_AGGREGATION_FUNCTION_TYPE_AVGRATE) {
      doubleValue = getAvgrateValue(doubleValue, metricSelection.getMetricAggregationInterval());
    }
    return doubleValue;
  }

  private static double getAvgrateValue(
      double originalValue, String metricAggregationIntervalPeriod) {
    long periodInSec = isoDurationToSeconds(metricAggregationIntervalPeriod);
    long oneSecInMillis = TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS);
    // avg rate hard coded for 1s
    double divisor = (double) TimeUnit.SECONDS.toMillis(periodInSec) / oneSecInMillis;
    return originalValue / divisor;
  }

  private List<Pair<Long, Double>> filterData(
      List<Pair<Long, Double>> dataList, long startTime, long endTime) {
    return dataList.stream()
        .filter(v -> v.getKey() >= startTime && v.getKey() <= endTime)
        .collect(Collectors.toList());
  }

  MetricTimeSeries getMetricTimeSeriesRecord(String tenantId, MetricSelection metricSelection) {
    return metricCache.getIfPresent(Pair.of(tenantId, metricSelection));
  }

  private void recordQueryTime(Instant startTime, String tenantId) {
    longQueryTimer
        .computeIfAbsent(
            tenantId,
            k -> PlatformMetricsRegistry.registerTimer(LONG_QUERY_TIMER, Map.of("tenantId", k)))
        .record(Duration.between(startTime, Instant.now()).toMillis(), TimeUnit.MILLISECONDS);
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
