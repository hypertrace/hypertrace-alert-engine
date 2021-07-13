package org.hypertrace.alert.engine.notification.transport.webhook.slack;

import java.util.List;

public class SectionBlock implements Block {
  public static final String TYPE = BlockType.SECTION.name().toLowerCase();
  private String type;
  private Text text;
  private String blockId;
  private List<Text> fields;
  private Element accessory;

  public SectionBlock() {
    this.type = TYPE;
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

  public String getBlockId() {
    return blockId;
  }

  public void setBlockId(String blockId) {
    this.blockId = blockId;
  }

  public List<Text> getFields() {
    return fields;
  }

  public void setFields(List<Text> fields) {
    this.fields = fields;
  }

  public Element getAccessory() {
    return accessory;
  }

  public void setAccessory(Element accessory) {
    this.accessory = accessory;
  }
}
