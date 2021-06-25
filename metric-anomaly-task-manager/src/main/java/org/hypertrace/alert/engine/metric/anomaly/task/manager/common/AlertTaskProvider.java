package org.hypertrace.alert.engine.metric.anomaly.task.manager.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Parser;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.AlertTask;
import org.hypertrace.alert.engine.eventcondition.config.service.v1.MetricAnomalyEventCondition;

public class AlertTaskProvider {
  static final String EVENT_CONDITION_ID = "eventConditionId";
  static final String EVENT_CONDITION_TYPE = "eventConditionType";
  static final String EVENT_CONDITION = "eventCondition";
  static final String METRIC_ANOMALY_EVENT_CONDITION = "MetricAnomalyEventCondition";
  static final int DEFAULT_DELAYED_TIME_IN_MIN = 1;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();

  public static List<Optional<AlertTask>> prepareTasks(List<JsonNode> rules) {
    List<Optional<AlertTask>> alertTasks =
        rules.stream()
            .map(
                rule -> {
                  Optional<AlertTask> optional = Optional.empty();
                  try {
                    AlertTask.Builder builder = AlertTask.newBuilder();
                    String eventConditionType = rule.get(EVENT_CONDITION_TYPE).textValue();
                    if (eventConditionType.equals(METRIC_ANOMALY_EVENT_CONDITION)) {
                      Instant current = roundHalfDown(Instant.now(), ChronoUnit.MINUTES);
                      builder.setCurrentExecutionTime(current.toEpochMilli());
                      builder.setLastExecutionTime(
                          current
                              .minus(Duration.of(DEFAULT_DELAYED_TIME_IN_MIN, ChronoUnit.MINUTES))
                              .toEpochMilli());
                      builder.setEventConditionId(rule.get(EVENT_CONDITION_ID).textValue());
                      String conditionType = rule.get(EVENT_CONDITION_TYPE).textValue();
                      if (!checkValidCondition(conditionType, rule.get(EVENT_CONDITION))) {
                        throw new RuntimeException("Invalid condition type value");
                      }
                      builder.setEventConditionType(rule.get(EVENT_CONDITION_TYPE).textValue());
                      builder.setEventConditionValue(buildValue(rule.get(EVENT_CONDITION)));
                    }
                    return Optional.of(builder.build());
                  } catch (Exception e) {
                    // skip this rule
                  }
                  return optional;
                })
            .collect(Collectors.toList());
    return alertTasks;
  }

  private static boolean checkValidCondition(String conditionType, JsonNode jsonNode) {
    boolean isValid = false;
    switch (conditionType) {
      case METRIC_ANOMALY_EVENT_CONDITION:
        try {
          JSON_PARSER.merge(
              OBJECT_MAPPER.writeValueAsString(jsonNode), MetricAnomalyEventCondition.newBuilder());
          isValid = true;
        } catch (Exception e) {
          // ignore
        }
        break;
      default:
        break;
    }
    return isValid;
  }

  private static Instant roundHalfDown(Instant instant, TemporalUnit unit) {
    return instant.minus(unit.getDuration().dividedBy(2)).truncatedTo(unit);
  }

  private static Value buildValue(JsonNode node) throws Exception {
    com.google.protobuf.Value.Builder valueBuilder = Value.newBuilder();
    JSON_PARSER.merge(OBJECT_MAPPER.writeValueAsString(node), valueBuilder);
    return valueBuilder.build();
  }
}
