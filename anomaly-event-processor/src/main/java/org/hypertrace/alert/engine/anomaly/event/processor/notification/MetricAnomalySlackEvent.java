package org.hypertrace.alert.engine.anomaly.event.processor.notification;

import static org.hypertrace.alert.engine.anomaly.event.processor.notification.SlackMessage.addIfNotEmpty;
import static org.hypertrace.alert.engine.anomaly.event.processor.notification.SlackMessage.addTimestamp;
import static org.hypertrace.alert.engine.anomaly.event.processor.notification.SlackMessage.getTitleBlock;

import java.util.ArrayList;
import java.util.List;
import org.hypertrace.alert.engine.notification.transport.webhook.slack.ActionBlock;
import org.hypertrace.alert.engine.notification.transport.webhook.slack.Attachment;
import org.hypertrace.alert.engine.notification.transport.webhook.slack.Button;
import org.hypertrace.alert.engine.notification.transport.webhook.slack.SectionBlock;
import org.hypertrace.alert.engine.notification.transport.webhook.slack.Text;

class MetricAnomalySlackEvent implements SlackMessage {

  public static final String EVENT_TIMESTAMP = "Event Timestamp";
  public static final String METRIC_ANOMALY_EVENT_TYPE = "Metric Anomaly Event Type";
  private final List<Attachment> attachments;

  MetricAnomalySlackEvent(List<Attachment> attachments) {
    this.attachments = attachments;
  }

  static MetricAnomalySlackEvent getMessage(
      MetricAnomalyWebhookEvent metricAnomalyWebhookEvent) {
    String titleMessage = "Alert for Metric Anomaly Event";
    SectionBlock titleBlock = getTitleBlock(titleMessage);

    // create Metadata block
    List<Text> metadataFields = new ArrayList<>();
    addTimestamp(metadataFields, metricAnomalyWebhookEvent.getEventTimeStamp(), EVENT_TIMESTAMP);
    addIfNotEmpty(
        metadataFields, metricAnomalyWebhookEvent.getEventConditionId(), METRIC_ANOMALY_EVENT_TYPE);

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

  List<Attachment> getAttachments() {
    return attachments;
  }
}
