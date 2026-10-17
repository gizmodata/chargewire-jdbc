package com.gizmodata.warpsql.jdbc.it;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Plain HTTP calls beside the driver, as another client of the same principal would make them:
 * for example ending the driver's session behind its back.
 */
final class RawWarp {
  private static final Pattern TOKEN = Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");

  private final HttpClient http = HttpClient.newHttpClient();
  private final WarpServer server;
  private final String token;

  RawWarp(WarpServer server) throws IOException, InterruptedException {
    this.server = server;
    String body = "{\"grant_type\":\"password\",\"username\":\"" + WarpServer.USER
        + "\",\"password\":\"" + server.password + "\"}";
    HttpResponse<String> response = http.send(
        HttpRequest.newBuilder(URI.create(server.httpBase() + "/warp/v1/auth/token"))
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
            HttpRequest.newBuilder(URI.create(server.httpBase() + "/warp/v1/sessions/" + sessionId))
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
}
