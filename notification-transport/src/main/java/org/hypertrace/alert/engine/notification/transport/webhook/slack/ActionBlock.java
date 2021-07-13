package org.hypertrace.alert.engine.notification.transport.webhook.slack;

import java.util.List;

public class ActionBlock implements Block {
  public static final String TYPE = BlockType.ACTIONS.name().toLowerCase();
  private String type;
  private List<Element> elements;
  private String blockId;

  public ActionBlock() {
    this.type = TYPE;
  }

  @Override
  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public List<Element> getElements() {
    return elements;
  }

  public void setElements(List<Element> elements) {
    this.elements = elements;
  }

  public String getBlockId() {
    return blockId;
  }

  public void setBlockId(String blockId) {
    this.blockId = blockId;
  }
}
