package org.hypertrace.alert.engine.metric.anomaly.task.manager.job;

import static org.hypertrace.alert.engine.metric.anomaly.task.manager.job.AlertTaskJobConstants.METRIC_ANOMALY_EVENT_CONDITION;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Parser;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Optional;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAnomalyEventCondition;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.AlertTask;
import org.hypertrace.core.documentstore.Document;

public class AlertTaskConverter {
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();

  static final String EVENT_CONDITION_ID = "eventConditionId";
  static final String EVENT_CONDITION_TYPE = "eventConditionType";
  static final String EVENT_CONDITION = "eventCondition";

  public static AlertTask toAlertTask(Document document, int delayInMinutes) throws IOException {
    JsonNode rule = OBJECT_MAPPER.readTree(document.toJson());

    AlertTask.Builder builder = AlertTask.newBuilder();
    String eventConditionType = rule.get(EVENT_CONDITION_TYPE).textValue();
    if (eventConditionType.equals(METRIC_ANOMALY_EVENT_CONDITION)) {
      Instant current = roundHalfDown(Instant.now(), ChronoUnit.MINUTES);
      builder.setCurrentExecutionTime(current.toEpochMilli());
      builder.setLastExecutionTime(
          current.minus(Duration.of(delayInMinutes, ChronoUnit.MINUTES)).toEpochMilli());
      builder.setEventConditionId(rule.get(EVENT_CONDITION_ID).textValue());
      String conditionType = rule.get(EVENT_CONDITION_TYPE).textValue();
      Optional<ByteBuffer> eventConditionValueAsBytes =
          getEventCondition(conditionType, rule.get(EVENT_CONDITION));
      if (!eventConditionValueAsBytes.isPresent()) {
        throw new RuntimeException("Invalid condition type value");
      }
      builder.setEventConditionType(rule.get(EVENT_CONDITION_TYPE).textValue());
      builder.setEventConditionValue(eventConditionValueAsBytes.get());
    }

    return builder.build();
  }

  private static Optional<ByteBuffer> getEventCondition(String conditionType, JsonNode jsonNode) {
    Optional<ByteBuffer> optionalByteBuffer = Optional.empty();
    switch (conditionType) {
      case METRIC_ANOMALY_EVENT_CONDITION:
        try {
          MetricAnomalyEventCondition.Builder builder = MetricAnomalyEventCondition.newBuilder();
          JSON_PARSER.merge(OBJECT_MAPPER.writeValueAsString(jsonNode), builder);
          MetricAnomalyEventCondition metricAnomalyEventCondition = builder.build();
          return Optional.of(ByteBuffer.wrap(metricAnomalyEventCondition.toByteArray()));
        } catch (Exception e) {
          // ignore
        }
        break;
      default:
        break;
    }
    return optionalByteBuffer;
  }

  private static Instant roundHalfDown(Instant instant, TemporalUnit unit) {
    return instant.minus(unit.getDuration().dividedBy(2)).truncatedTo(unit);
  }
}
