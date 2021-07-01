package org.hypertrace.alert.engine.metric.anomaly.detector;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
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
import org.hypertrace.core.attribute.service.client.config.AttributeServiceClientConfig;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeMetadataFilter;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.core.query.service.client.QueryServiceConfig;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

public class MetricQueryBuilderTest {

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
  }

  @Test
  void temp() throws URISyntaxException, MalformedURLException {

    Config config =
        ConfigFactory.parseURL(
            Thread.currentThread()
                .getContextClassLoader()
                .getResource("application.conf")
                .toURI()
                .toURL());
    AttributeServiceClientConfig asConfig = AttributeServiceClientConfig.from(config);
    ManagedChannel attributeServiceChannel =
        ManagedChannelBuilder.forAddress(asConfig.getHost(), asConfig.getPort())
            .usePlaintext()
            .build();
    AttributeServiceClient asClient = new AttributeServiceClient(attributeServiceChannel);

    Config qsConfig = config.getConfig("query.service.config");
    QueryServiceClient queryServiceClient =
        new QueryServiceClient(new QueryServiceConfig(qsConfig));

    MetricQueryBuilder metricQueryBuilder = new MetricQueryBuilder(asClient);

    LhsExpression lhsExpression =
        LhsExpression.newBuilder()
            .setAttribute(Attribute.newBuilder().setKey("id").setScope("SERVICE").build())
            .build();
    RhsExpression rhsExpression =
        RhsExpression.newBuilder().setStringValue("5efcdda0-2a81-3c19-bf66-2187891bdba1").build();
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

    QueryRequest queryRequest =
        metricQueryBuilder.buildMetricQueryRequest(
            metricSelection,
            System.currentTimeMillis() - Duration.ofHours(2).toMillis(),
            System.currentTimeMillis(),
            "__default");

    Iterator<ResultSetChunk> resultSetChunkIterator =
        queryServiceClient.executeQuery(queryRequest, Map.of("x-tenant-id", "__default"), 100000);

    List<ResultSetChunk> list = new ArrayList<>();
    resultSetChunkIterator.forEachRemaining(list::add);

    int x = 1;

    /**
     * Select dateTimeConvert(start_time_millis,?,?,?), SUM(duration_millis) FROM rawServiceView
     * WHERE tenant_id = ? AND ( ( start_time_millis >= ? AND start_time_millis < ? ) AND service_id
     * = ? ) GROUP BY dateTimeConvert(start_time_millis,?,?,?) limit 10000
     *
     * <p>Select dateTimeConvert(start_time_millis,?,?,?), SUM(duration_millis) FROM rawServiceView
     * WHERE tenant_id = ? AND ( ( start_time_millis >= ? AND start_time_millis < ? ) AND service_id
     * = ? ) GROUP BY dateTimeConvert(start_time_millis,?,?,?) limit 10000 ->
     * "Params{integerParams={}, longParams={4=1624970911920, 5=1624978111920},
     * stringParams={0=1:MILLISECONDS:EPOCH, 1=1:MILLISECONDS:EPOCH, 2=15:SECONDS, 3=default,
     * 6=b89ddf9b-c582-3d93-9006-a4b3024cf5a5, 7=1:MILLISECONDS:EPOCH, 8=1:MILLISECONDS:EPOCH,
     * 9=15:SECONDS}, floatParams={}, doubleParams={}, byteStringParams={}}"
     *
     * <p>Select
     * dateTimeConvert(start_time_millis,'1:MILLISECONDS:EPOCH','1:MILLISECONDS:EPOCH','15:SECONDS'),
     * SUM(duration_millis) FROM rawServiceView WHERE tenant_id = 'default' AND ( (
     * start_time_millis >= 1624970911920 AND start_time_millis < 1624978111920 ) AND service_id =
     * '5efcdda0-2a81-3c19-bf66-2187891bdba1' ) GROUP BY
     * dateTimeConvert(start_time_millis,'1:MILLISECONDS:EPOCH','1:MILLISECONDS:EPOCH','15:SECONDS')
     * limit 10000
     */
  }
}
