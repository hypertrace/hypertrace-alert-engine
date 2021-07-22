package org.hypertrace.alert.engine.notification.transport.webhook;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Optional;
import okhttp3.Response;
import org.hypertrace.alert.engine.notification.transport.webhook.http.HttpWithJsonSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Sender specific to slack. This class understand different statuses and response from slack */
public class WebhookSender {
  private static final int OK_CODE = 200;
  private static final Logger LOGGER = LoggerFactory.getLogger(WebhookSender.class);
  private final HttpWithJsonSender sender;

  public WebhookSender(HttpWithJsonSender sender) {
    this.sender = sender;
  }

  // Received Notification Message and converts to json string to meet Slack specs
  public void send(String url, Object obj) {
    Preconditions.checkArgument(url != null);
    ObjectMapper objectMapper = ObjectMapperProvider.get();
    String jsonString;
    try {
      jsonString = objectMapper.writeValueAsString(obj);
    } catch (IOException e) {
      LOGGER.error(
          "Failed to send notification due to JSON serialization. Slack Message to serialize: {} \n",
          obj,
          e);
      // don't even bother sending to slack
      return;
    }

    Optional<Response> responseOptional = sender.send(url, jsonString);

    if (responseOptional.isEmpty()) {
      LOGGER.error("Failed sending Slack Message: {} \n", jsonString);
      return;
    }
    Response response = responseOptional.get();
    int responseCode = response.code();
    LOGGER.info("Code to logger: " + response.code());
    if (responseCode != OK_CODE) {
      LOGGER.error(
          "Error response from Slack when attempting to send notification. "
              + "Response Code: {}, Response Message: {} \n Attempted Notification: {}",
          responseCode,
          response.message(),
          jsonString);
    }
  }
}
