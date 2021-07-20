package org.hypertrace.alert.engine.notification.transport.webhook;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ObjectMapperProvider {
  private static ObjectMapper objectMapper;

  public static ObjectMapper get() {
    if (objectMapper == null) {
      synchronized (ObjectMapperProvider.class) {
        if (objectMapper == null) {
          objectMapper = new ObjectMapper().setSerializationInclusion(Include.NON_NULL);
        }
      }
    }
    return objectMapper;
  }
}
