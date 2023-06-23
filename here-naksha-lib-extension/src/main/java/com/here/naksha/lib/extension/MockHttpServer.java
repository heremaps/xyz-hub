package com.here.naksha.lib.extension;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import org.jetbrains.annotations.NotNull;

public class MockHttpServer {

  public final @NotNull HttpServer server;

  public MockHttpServer(int port) throws IOException {
    server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext("/", new MyHandler());
    server.setExecutor(null); // Use the default executor
    server.start();
    System.out.println("Server running on port " + port);
  }

  static class MyHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      String response = """
{"type":"HealthStatus","status":"OK"}""";
      exchange.sendResponseHeaders(200, response.length());
      OutputStream outputStream = exchange.getResponseBody();
      outputStream.write(response.getBytes());
      outputStream.close();
    }
  }
}
