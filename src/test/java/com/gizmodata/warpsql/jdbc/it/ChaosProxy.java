package com.gizmodata.warpsql.jdbc.it;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * A TCP proxy in front of a server that breaks connections on purpose: after a request matching a
 * pattern goes through, it lets a given number of response bytes back and then closes both sides,
 * the way a proxy timeout or a network failure would. It breaks the first {@code times} such
 * responses and forwards everything else untouched.
 */
final class ChaosProxy implements AutoCloseable {
  private final ServerSocket listener;
  private final int targetPort;
  private final Pattern request;
  private final long cutAfterBytes;
  private final AtomicInteger cutsLeft;
  private final AtomicInteger cuts = new AtomicInteger();
  private final AtomicLong bytesFromServer = new AtomicLong();

  ChaosProxy(int targetPort, String requestPattern, long cutAfterBytes, int times)
      throws IOException {
    this.listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
    this.targetPort = targetPort;
    this.request = Pattern.compile(requestPattern);
    this.cutAfterBytes = cutAfterBytes;
    this.cutsLeft = new AtomicInteger(times);
    Thread acceptor = new Thread(this::accept, "chaos-proxy");
    acceptor.setDaemon(true);
    acceptor.start();
  }

  int port() {
    return listener.getLocalPort();
  }

  /** How many responses the proxy has broken. */
  int cuts() {
    return cuts.get();
  }

  /** How many bytes the server has sent through the proxy. */
  long bytesFromServer() {
    return bytesFromServer.get();
  }

  private void accept() {
    while (!listener.isClosed()) {
      Socket client;
      try {
        client = listener.accept();
      } catch (IOException e) {
        return;
      }
      try {
        Socket server = new Socket(InetAddress.getLoopbackAddress(), targetPort);
        Connection connection = new Connection(client, server);
        start(connection::clientToServer);
        start(connection::serverToClient);
      } catch (IOException e) {
        try {
          client.close();
        } catch (IOException closing) {
          // The client sees the connection drop, as it would without the proxy.
        }
      }
    }
  }

  private static void start(Runnable task) {
    Thread thread = new Thread(task, "chaos-proxy-pipe");
    thread.setDaemon(true);
    thread.start();
  }

  /** One proxied connection; HTTP/1.1 sends one request at a time on it. */
  private final class Connection {
    private final Socket client;
    private final Socket server;
    private volatile long armedAt = -1;
    private long fromServer;

    Connection(Socket client, Socket server) {
      this.client = client;
      this.server = server;
    }

    void clientToServer() {
      StringBuilder recent = new StringBuilder();
      try (InputStream in = client.getInputStream();
          OutputStream out = server.getOutputStream()) {
        byte[] buffer = new byte[16384];
        for (int read; (read = in.read(buffer)) >= 0; ) {
          recent.append(new String(buffer, 0, read, StandardCharsets.ISO_8859_1));
          if (recent.length() > 8192) {
            recent.delete(0, recent.length() - 8192);
          }
          if (armedAt < 0 && request.matcher(recent).find() && cutsLeft.getAndDecrement() > 0) {
            armedAt = fromServerSnapshot();
            recent.setLength(0);
          }
          out.write(buffer, 0, read);
          out.flush();
        }
      } catch (IOException e) {
        // The other direction closed the connection.
      } finally {
        closeBoth();
      }
    }

    private synchronized long fromServerSnapshot() {
      return fromServer;
    }

    void serverToClient() {
      try (InputStream in = server.getInputStream();
          OutputStream out = client.getOutputStream()) {
        byte[] buffer = new byte[16384];
        for (int read; (read = in.read(buffer)) >= 0; ) {
          int allowed = read;
          synchronized (this) {
            long armed = armedAt;
            if (armed >= 0 && fromServer + read - armed > cutAfterBytes) {
              allowed = (int) Math.max(0, cutAfterBytes - (fromServer - armed));
            }
            fromServer += read;
          }
          bytesFromServer.addAndGet(allowed);
          out.write(buffer, 0, allowed);
          out.flush();
          if (allowed < read) {
            cuts.incrementAndGet();
            return;
          }
        }
      } catch (IOException e) {
        // The other direction closed the connection.
      } finally {
        closeBoth();
      }
    }

    private void closeBoth() {
      try {
        client.close();
      } catch (IOException e) {
        // Already closed.
      }
      try {
        server.close();
      } catch (IOException e) {
        // Already closed.
      }
    }
  }

  @Override
  public void close() throws IOException {
    listener.close();
  }
}
