package org.hypertrace.alert.engine.notification.service.notification;

import com.google.common.base.Strings;
import java.time.Instant;
import java.util.List;
import org.hypertrace.alert.engine.notification.transport.webhook.slack.SectionBlock;
import org.hypertrace.alert.engine.notification.transport.webhook.slack.Text;

public interface SlackMessage {

  static SectionBlock getTitleBlock(String titleMessage) {
    Text titleText = new Text(Text.MARKDOWN_TYPE, titleMessage);
    SectionBlock titleBlock = new SectionBlock();
    titleBlock.setText(titleText);
    return titleBlock;
  }

  static void addIfNotEmpty(List<Text> metadataFields, String value, String type) {
    if (!Strings.isNullOrEmpty(value)) {
      metadataFields.add(new Text(Text.MARKDOWN_TYPE, "*" + type + ":*\n" + value));
    }
  }

  static void addTimestamp(List<Text> metadataFields, Instant value, String type) {
    if (!Strings.isNullOrEmpty(type)) {
      String currentDate = value.toString();
      long epochSeconds = value.getEpochSecond() - 19800;
      metadataFields.add(
          new Text(
              Text.MARKDOWN_TYPE,
              "*"
                  + type
                  + ": *\n"
                  + "<!date^"
                  + epochSeconds
                  + "^"
                  + "{date_num} {time_secs}| "
                  + currentDate
                  + " UTC >"));
    }
  }
}
