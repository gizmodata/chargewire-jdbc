package com.gizmodata.chargewire.jdbc.client;

import java.awt.Desktop;
import java.awt.GraphicsEnvironment;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Opens a browser sign-in page (spec §4.3): with the configured command, else the desktop's
 * browser, else the system's opener; when none works, or the command is {@code none}, the URL is
 * printed for the person to open.
 *
 * <p>Internal API.
 */
final class BrowserOpener {
  private BrowserOpener() {}

  /**
   * Shows the person {@code url}.
   *
   * @param command the configured command (the URL is appended), {@code none}, or {@code null}
   * @param url the identity provider's authorize URL (it holds no secret)
   * @param what what the person signs in to, for the message
   * @param err where messages go
   */
  static void open(String command, String url, String what, PrintStream err) {
    if (command != null && "none".equals(command.toLowerCase(Locale.ROOT))) {
      print(url, what, err);
      return;
    }
    try {
      if (command != null) {
        run(command, url);
      } else if (!GraphicsEnvironment.isHeadless()
          && Desktop.isDesktopSupported()
          && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
        Desktop.getDesktop().browse(URI.create(url));
      } else {
        run(systemOpener(), url);
      }
      err.println("Waiting for the browser sign-in to " + what + "...");
    } catch (IOException | RuntimeException | LinkageError e) {
      print(url, what, err);
    }
  }

  private static void print(String url, String what, PrintStream err) {
    err.println("Open this URL in a browser to sign in to " + what + ":");
    err.println(url);
  }

  private static String systemOpener() {
    String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    if (os.contains("mac")) {
      return "open";
    }
    if (os.contains("win")) {
      return "rundll32 url.dll,FileProtocolHandler";
    }
    return "xdg-open";
  }

  private static void run(String command, String url) throws IOException {
    List<String> args = new ArrayList<>(List.of(command.trim().split("\\s+")));
    args.add(url);
    // Not waited for: a browser started by its command often runs until it is closed.
    new ProcessBuilder(args)
        .redirectOutput(ProcessBuilder.Redirect.DISCARD)
        .redirectError(ProcessBuilder.Redirect.DISCARD)
        .start();
  }
}
