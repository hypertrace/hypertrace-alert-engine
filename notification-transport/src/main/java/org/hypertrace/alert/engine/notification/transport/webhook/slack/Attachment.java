package org.hypertrace.alert.engine.notification.transport.webhook.slack;

import java.util.List;

/** POJO to seralize Slack Attachment message */
public class Attachment {
  public static final String RED = "#d41729";
  private String color;
  private List<Block> blocks;

  public String getColor() {
    return color;
  }

  public void setColor(String color) {
    this.color = color;
  }

  public List<Block> getBlocks() {
    return blocks;
  }

  public void setBlocks(List<Block> blocks) {
    this.blocks = blocks;
  }
}
