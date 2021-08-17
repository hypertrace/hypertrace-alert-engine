package org.hypertrace.alert.engine.notification.service.notification;

import static org.hypertrace.alert.engine.notification.service.notification.SlackMessage.addIfNotEmpty;
import static org.hypertrace.alert.engine.notification.service.notification.SlackMessage.addTimestamp;
import static org.hypertrace.alert.engine.notification.service.notification.SlackMessage.getTitleBlock;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.BaselineRuleViolationSummary;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.StaticRuleViolationSummary;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.StaticThresholdOperator;
import org.hypertrace.alert.engine.metric.anomaly.datamodel.ViolationSummary;
import org.hypertrace.alert.engine.notification.transport.webhook.slack.ActionBlock;
import org.hypertrace.alert.engine.notification.transport.webhook.slack.Attachment;
import org.hypertrace.alert.engine.notification.transport.webhook.slack.Button;
import org.hypertrace.alert.engine.notification.transport.webhook.slack.SectionBlock;
import org.hypertrace.alert.engine.notification.transport.webhook.slack.Text;

public class MetricAnomalySlackEvent implements SlackMessage {

  public static final String EVENT_TIMESTAMP = "Event Timestamp";
  public static final String VIOLATION_TIMESTAMP = "Violation Timestamp";
  public static final String METRIC_ANOMALY_EVENT_ID = "MetricAnomalyEvent Id";
  public static final String VIOLATION_SUMMARY = "Violation Summary";
  private final List<Attachment> attachments;

  public MetricAnomalySlackEvent(List<Attachment> attachments) {
    this.attachments = attachments;
  }

  public static MetricAnomalySlackEvent getMessage(
      MetricAnomalyWebhookEvent metricAnomalyWebhookEvent) {
    String titleMessage = "Hypertrace detected MetricAnomalyEvent violation";
    SectionBlock titleBlock = getTitleBlock(titleMessage);

    // create Metadata block
    List<Text> metadataFields = new ArrayList<>();
    addTimestamp(metadataFields, metricAnomalyWebhookEvent.getEventTimestamp(), EVENT_TIMESTAMP);
    addTimestamp(
        metadataFields, metricAnomalyWebhookEvent.getViolationTimestamp(), VIOLATION_TIMESTAMP);
    addIfNotEmpty(
        metadataFields, metricAnomalyWebhookEvent.getEventConditionId(), METRIC_ANOMALY_EVENT_ID);
    addIfNotEmpty(
        metadataFields,
        getViolationSummary(metricAnomalyWebhookEvent.getViolationSummaryList()),
        VIOLATION_SUMMARY);

    SectionBlock metadataBlock = new SectionBlock();
    metadataBlock.setFields(metadataFields);

    // create Action Block
    // todo do we need a button?
    Text buttonText = new Text(Text.PLAINTEXT_TYPE, "See Event_");
    Button button = new Button(buttonText);
    button.setValue(metricAnomalyWebhookEvent.getEventConditionId());
    button.setStyle(Button.PRIMARY_STYLE);
    button.setUrl("empty url");
    ActionBlock actionBlock = new ActionBlock();
    actionBlock.setType(ActionBlock.TYPE);
    actionBlock.setElements(List.of(button));

    Attachment attachments = new Attachment();
    attachments.setColor(Attachment.RED);
    attachments.setBlocks(List.of(titleBlock, metadataBlock));
    return new MetricAnomalySlackEvent(List.of(attachments));
  }

  public List<Attachment> getAttachments() {
    return attachments;
  }

  private static String getViolationSummary(List<ViolationSummary> violationSummaryList) {
    return violationSummaryList.stream()
        .map(
            violationSummary -> {
              if (violationSummary.getViolationSummary() instanceof StaticRuleViolationSummary) {
                return getMessageStringForStaticRule(
                    (StaticRuleViolationSummary) violationSummary.getViolationSummary());
              } else {
                return getMessageStringForDynamicRule(
                    (BaselineRuleViolationSummary) violationSummary.getViolationSummary());
              }
            })
        .collect(Collectors.joining("\n"));
  }

  private static String getMessageStringForStaticRule(StaticRuleViolationSummary violationSummary) {
    return String.format(
        "%d out of %d metric data points were %s than the static threshold %f in the duration %s.",
        violationSummary.getViolationCount(),
        violationSummary.getDataCount(),
        getStringFromOperator(violationSummary.getOperator()),
        violationSummary.getStaticThreshold(),
        violationSummary.getRuleDuration());
  }

  private static String getMessageStringForDynamicRule(
      BaselineRuleViolationSummary violationSummary) {
    return String.format(
        "%d out of %d metric data points were outside the dynamic baseline bounds [%f, %f] in the duration %s.",
        violationSummary.getViolationCount(),
        violationSummary.getDataCount(),
        violationSummary.getBaselineLowerBound(),
        violationSummary.getBaselineUpperBound(),
        violationSummary.getRuleDuration());
  }

  private static String getStringFromOperator(StaticThresholdOperator operator) {
    switch (operator) {
      case STATIC_THRESHOLD_OPERATOR_GT:
        return "greater";
      case STATIC_THRESHOLD_OPERATOR_LT:
        return "less";
      case STATIC_THRESHOLD_OPERATOR_GTE:
        return "greater than or equal to";
      case STATIC_THRESHOLD_OPERATOR_LTE:
        return "less than or equal to";
      default:
        throw new UnsupportedOperationException(
            "Unsupported threshold condition operator: " + operator);
    }
  }
}
