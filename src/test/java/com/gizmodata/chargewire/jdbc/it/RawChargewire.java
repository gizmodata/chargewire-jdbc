package com.gizmodata.chargewire.jdbc.it;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Plain HTTP calls beside the driver, as another client of the same principal would make them: for
 * example ending the driver's session behind its back.
 */
final class RawChargewire {
  private static final Pattern TOKEN = Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");
  private static final Pattern SESSION = Pattern.compile("\"session_id\"\\s*:\\s*\"([^\"]+)\"");

  private final HttpClient http = HttpClient.newHttpClient();
  private final ChargewireServer server;
  private final String token;

  RawChargewire(ChargewireServer server) throws IOException, InterruptedException {
    this.server = server;
    String body =
        "{\"grant_type\":\"password\",\"username\":\""
            + ChargewireServer.USER
            + "\",\"password\":\""
            + server.password
            + "\"}";
    HttpResponse<String> response =
        http.send(
            HttpRequest.newBuilder(URI.create(server.httpBase() + "/chargewire/v1/auth/token"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    Matcher matcher = TOKEN.matcher(response.body());
    if (!matcher.find()) {
      throw new IllegalStateException("No token: " + response.body());
    }
    this.token = matcher.group(1);
  }

  String token() {
    return token;
  }

  int deleteSession(String sessionId) throws IOException, InterruptedException {
    return http.send(
            HttpRequest.newBuilder(
                    URI.create(server.httpBase() + "/chargewire/v1/sessions/" + sessionId))
                .header("Authorization", "Bearer " + token)
                .DELETE()
                .build(),
            HttpResponse.BodyHandlers.discarding())
        .statusCode();
  }

  HttpResponse<String> get(String path) throws IOException, InterruptedException {
    return http.send(
        HttpRequest.newBuilder(URI.create(server.httpBase() + path))
            .header("Authorization", "Bearer " + token)
            .GET()
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  /** Opens a session of its own. */
  String createSession() throws IOException, InterruptedException {
    HttpResponse<String> response =
        http.send(
            HttpRequest.newBuilder(URI.create(server.httpBase() + "/chargewire/v1/sessions"))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{}"))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    Matcher matcher = SESSION.matcher(response.body());
    if (!matcher.find()) {
      throw new IllegalStateException("No session: " + response.body());
    }
    return matcher.group(1);
  }

  /**
   * Runs a query on the streamed path, without asking for partitions, and returns how many rows its
   * first record batch has: the server's batch size.
   */
  long firstBatchRows(String sql) throws IOException, InterruptedException {
    String session = createSession();
    try {
      HttpResponse<byte[]> response =
          http.send(
              HttpRequest.newBuilder(
                      URI.create(
                          server.httpBase() + "/chargewire/v1/sessions/" + session + "/statements"))
                  .header("Authorization", "Bearer " + token)
                  .header("Content-Type", "application/json")
                  .header("Accept", "application/vnd.apache.arrow.stream")
                  .POST(
                      HttpRequest.BodyPublishers.ofString(
                          "{\"sql\":\"" + sql.replace("\"", "\\\"") + "\"}"))
                  .build(),
              HttpResponse.BodyHandlers.ofByteArray());
      if (response.statusCode() != 200) {
        throw new IllegalStateException("Status " + response.statusCode());
      }
      return firstBatchRows(response.body());
    } finally {
      deleteSession(session);
    }
  }

  /**
   * Reads the row count of an Arrow IPC stream's first record batch from its flatbuffer metadata
   * (the tests have no Arrow library: the driver's is relocated inside its jar).
   */
  static long firstBatchRows(byte[] stream) {
    ByteBuffer buffer = ByteBuffer.wrap(stream).order(ByteOrder.LITTLE_ENDIAN);
    int position = 0;
    while (position + 8 <= stream.length) {
      int length = buffer.getInt(position);
      if (length == -1) {
        length = buffer.getInt(position + 4);
        position += 8;
      } else {
        position += 4;
      }
      if (length == 0) {
        break;
      }
      int message = position + buffer.getInt(position);
      int vtable = message - buffer.getInt(message);
      int typeField = field(buffer, vtable, 1);
      int bodyField = field(buffer, vtable, 3);
      int headerType = typeField == 0 ? 0 : buffer.get(message + typeField);
      long bodyLength = bodyField == 0 ? 0 : buffer.getLong(message + bodyField);
      if (headerType == 3) {
        int at = message + field(buffer, vtable, 2);
        int batch = at + buffer.getInt(at);
        int offset = field(buffer, batch - buffer.getInt(batch), 0);
        return offset == 0 ? 0 : buffer.getLong(batch + offset);
      }
      position += length + (int) bodyLength;
    }
    throw new IllegalStateException("The stream has no record batch");
  }

  /** The offset of a flatbuffer table's field from the table, or 0 when the field is absent. */
  private static int field(ByteBuffer buffer, int vtable, int index) {
    int slot = 4 + 2 * index;
    int size = buffer.getShort(vtable) & 0xffff;
    return slot < size ? buffer.getShort(vtable + slot) & 0xffff : 0;
  }
}
