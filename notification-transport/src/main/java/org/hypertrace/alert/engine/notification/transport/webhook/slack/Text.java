package org.hypertrace.alert.engine.notification.transport.webhook.slack;

public class Text {
  public static final String MARKDOWN_TYPE = "mrkdwn";
  public static final String PLAINTEXT_TYPE = "plain_text";
  private String type;
  private String text;

  public Text(String type, String text) {
    this.type = type;
    this.text = text;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }
}
