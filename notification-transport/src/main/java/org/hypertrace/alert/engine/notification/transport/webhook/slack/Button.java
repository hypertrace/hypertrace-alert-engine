package org.hypertrace.alert.engine.notification.transport.webhook.slack;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Button implements Element {
  public static final String TYPE = "button";
  public static final String PRIMARY_STYLE = "primary";
  private String type;
  private Text text;
  private String actionId;
  private String value;
  private String url;
  private String style;

  public Button(Text text) {
    this.type = TYPE;
    this.text = text;
  }

  @Override
  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public Text getText() {
    return text;
  }

  public void setText(Text text) {
    this.text = text;
  }

  @JsonProperty("action_id")
  public String getActionId() {
    return actionId;
  }

  public void setActionId(String actionId) {
    this.actionId = actionId;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getStyle() {
    return style;
  }

  public void setStyle(String style) {
    this.style = style;
  }
}
