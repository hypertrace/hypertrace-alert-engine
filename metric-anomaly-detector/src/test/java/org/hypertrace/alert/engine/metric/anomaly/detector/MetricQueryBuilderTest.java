package org.hypertrace.alert.engine.metric.anomaly.detector;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.cache.CacheStats;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.Attribute;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.Filter;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.LeafFilter;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.LhsExpression;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAggregationFunction;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricSelection;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.RhsExpression;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.ValueOperator;
import org.hypertrace.core.attribute.service.client.AttributeServiceClient;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeMetadataFilter;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

class MetricQueryBuilderTest {

  @Test
  void testBuildMetricQueryRequest() {
    LhsExpression lhsExpression =
        LhsExpression.newBuilder()
            .setAttribute(Attribute.newBuilder().setKey("id").setScope("SERVICE").build())
            .build();
    RhsExpression rhsExpression = RhsExpression.newBuilder().setStringValue("1234").build();
    LeafFilter leafFilter =
        LeafFilter.newBuilder()
            .setValueOperator(ValueOperator.VALUE_OPERATOR_EQ)
            .setLhsExpression(lhsExpression)
            .setRhsExpression(rhsExpression)
            .build();

    MetricSelection metricSelection =
        MetricSelection.newBuilder()
            .setMetricAggregationInterval("PT15s")
            .setMetricAggregationFunction(
                MetricAggregationFunction.METRIC_AGGREGATION_FUNCTION_TYPE_SUM)
            .setFilter(Filter.newBuilder().setLeafFilter(leafFilter).build())
            .setMetricAttribute(
                Attribute.newBuilder().setKey("duration").setScope("SERVICE").build())
            .build();

    AttributeServiceClient attributesServiceClient = mock(AttributeServiceClient.class);
    List<AttributeMetadata> attributesList1 =
        List.of(
            AttributeMetadata.newBuilder()
                .setScopeString(AttributeScope.SERVICE.name())
                .setKey("id")
                .setId("Service.id")
                .build(),
            AttributeMetadata.newBuilder()
                .setScopeString(AttributeScope.SERVICE.name())
                .setKey("startTime")
                .setId("Service.startTime")
                .build());
    when(attributesServiceClient.findAttributes(
            eq(Map.of("x-tenant-id", "__default")),
            eq(
                AttributeMetadataFilter.newBuilder()
                    .addScopeString(AttributeScope.SERVICE.name())
                    .build())))
        .thenAnswer((Answer<Iterator<AttributeMetadata>>) invocation -> attributesList1.iterator());

    MetricQueryBuilder mqb = new MetricQueryBuilder(attributesServiceClient);

    QueryRequest queryRequest = mqb.buildMetricQueryRequest(metricSelection, 100, 200, "__default");

    Assertions.assertNotNull(queryRequest);
  }

  @Test
  void testAttributeCaching() {
    List<AttributeMetadata> attributesList1 =
        List.of(
            AttributeMetadata.newBuilder()
                .setScopeString(AttributeScope.SERVICE.name())
                .setKey("id")
                .setId("Service.id")
                .build(),
            AttributeMetadata.newBuilder()
                .setScopeString(AttributeScope.SERVICE.name())
                .setKey("startTime")
                .setId("Service.startTime")
                .build());

    AttributeServiceClient attributesServiceClient = mock(AttributeServiceClient.class);
    when(attributesServiceClient.findAttributes(
            eq(Map.of("x-tenant-id", "__default")),
            eq(
                AttributeMetadataFilter.newBuilder()
                    .addScopeString(AttributeScope.SERVICE.name())
                    .build())))
        .thenAnswer((Answer<Iterator<AttributeMetadata>>) invocation -> attributesList1.iterator());

    MetricQueryBuilder mqb = new MetricQueryBuilder(attributesServiceClient);
    CacheStats stats = mqb.attributeServiceCache.stats();

    // Cache stats before adding any key
    Assertions.assertEquals(0, stats.missCount());
    Assertions.assertEquals(0, stats.hitCount());

    String beforeVal = mqb.getTimestampAttributeId("__default", "SERVICE");
    stats = mqb.attributeServiceCache.stats();

    // Cache stats after adding a key
    Assertions.assertEquals(1, stats.missCount());
    Assertions.assertEquals(0, stats.hitCount());

    String afterVal = mqb.getTimestampAttributeId("__default", "SERVICE");
    Assertions.assertEquals(beforeVal, afterVal);
    stats = mqb.attributeServiceCache.stats();

    // Cache stats after calling a cached key
    Assertions.assertEquals(1, stats.missCount());
    Assertions.assertEquals(1, stats.hitCount());
  }
}
