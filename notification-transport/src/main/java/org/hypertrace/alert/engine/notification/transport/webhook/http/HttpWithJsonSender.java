package org.hypertrace.alert.engine.notification.transport.webhook.http;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Optional;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a generic Http Sender to send JSON String to any specified URL. This should be a
 * stateless object that it's job is to send json string
 */
public class HttpWithJsonSender {
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpWithJsonSender.class);
  public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
  private final OkHttpClient client;
  private static final HttpWithJsonSender INSTANCE = new HttpWithJsonSender(new OkHttpClient());

  @VisibleForTesting
  HttpWithJsonSender(OkHttpClient client) {
    this.client = client;
  }

  public static HttpWithJsonSender getInstance() {
    return INSTANCE;
  }

  public Optional<Response> send(String url, String jsonString) {
    LOGGER.info("Sending the following json string: {}", jsonString);
    RequestBody body = RequestBody.create(jsonString, JSON);
    Request request = new Request.Builder().url(url).post(body).build();
    try (Response response = client.newCall(request).execute()) {
      return Optional.of(response);
    } catch (IOException ioe) {
      LOGGER.error("Unable to send json string to URL: {}, with message: {}", url, jsonString, ioe);
    }
    return Optional.empty();
  }
}
