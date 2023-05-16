package com.here.naksha.http;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.here.xyz.IoEventPipeline;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.feature.GetFeaturesByIdEvent;
import com.here.xyz.events.info.HealthCheckEvent;
import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.HealthStatus;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.Params;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class HttpHandlerTest {

  static Connector connector;
  static IoEventPipeline eventPipeline;
  static HttpHandler httpHandler;

  @BeforeAll
  static void setup() throws XyzErrorException, IOException {
    connector = new Connector("test:http", Math.abs(RandomUtils.nextLong()));
    String url = "http://localhost:9999/";
    try {
      // Set the env var below if you want to run the test against a specific endpoint
      final String rawUrl = System.getenv("HTTP_HANDLER_TEST_URI");
      final URL goodURL = new URL(rawUrl);
      url = goodURL.toString();
    } catch (Exception ignore) {
    }
    connector.setParams(new Params()
        .with(HttpHandlerParams.URL, url)
        .with(HttpHandlerParams.HTTP_METHOD, HttpHandlerParams.HTTP_GET)
    );
    eventPipeline = new IoEventPipeline();
    httpHandler = new HttpHandler(connector);
    eventPipeline.addEventHandler(httpHandler);
    fakeWebserver = new HttpFakeWebserver(9999);
  }

  @AfterAll
  public static void stopWebServer() {
    if (fakeWebserver != null) {
      fakeWebserver.server.stop(0);
      fakeWebserver = null;
    }
  }

  static HttpFakeWebserver fakeWebserver;

  @Test
  void test_GetFeaturesById() throws IOException {
    final GetFeaturesByIdEvent event = new GetFeaturesByIdEvent();
    final List<String> ids = new ArrayList<>();
    ids.add("a");
    ids.add("b");
    event.setIds(ids);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    eventPipeline.sendEvent(new ByteArrayInputStream(event.toByteArray()), out);
    final XyzResponse response = XyzSerializable.deserialize(out.toByteArray(), XyzResponse.class);
    assertNotNull(response);
    final ErrorResponse errorResponse = assertInstanceOf(ErrorResponse.class, response);
    assertSame(XyzError.NOT_IMPLEMENTED, errorResponse.getError());
  }

  @Test
  public void test_HealthCheckEvent() throws IOException {
    final HealthCheckEvent event = new HealthCheckEvent();
    event.setSpace(new Space("ASpaceThatShouldNotExistBecauseWeAreTesting"));
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    eventPipeline.sendEvent(new ByteArrayInputStream(event.toByteArray()), out);
    //    eventPipeline.sendEvent(IoHelp.openResource("testevent.json"), out);
    final XyzResponse response = XyzSerializable.deserialize(out.toByteArray(), XyzResponse.class);
    assertNotNull(response);
    assertInstanceOf(HealthStatus.class, response);
  }
}