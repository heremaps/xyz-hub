package com.here.naksha.activitylog;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.here.xyz.IoEventPipeline;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.feature.GetFeaturesByIdEvent;
import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.IoHelp;
import com.here.xyz.util.Params;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class HttpHandlerTest {

  static Connector connector;
  static IoEventPipeline eventPipeline;
  static HttpHandler httpHandler;

  @BeforeAll
  static void setup() throws XyzErrorException {
    connector = new Connector("test:http", Math.abs(RandomUtils.nextLong()));
    connector.setParams(new Params().with(HttpHandlerParams.URL,System.getenv("FOO")));
    eventPipeline = new IoEventPipeline();
    httpHandler = new HttpHandler(connector);
    eventPipeline.addEventHandler(httpHandler);
  }

  @Test
  void test_GetFeaturesById() throws IOException {
    final GetFeaturesByIdEvent event = new GetFeaturesByIdEvent();
    final List<String> ids = new ArrayList<>();
    ids.add("a");
    ids.add("b");
    event.setIds(ids);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
//    eventPipeline.sendEvent(IoHelp.openResource("testevent.json"), out);
    eventPipeline.sendEvent(new ByteArrayInputStream(event.toByteArray()), out);
    final XyzResponse response = XyzSerializable.deserialize(out.toByteArray(), XyzResponse.class);
    assertNotNull(response);
    assertInstanceOf(ErrorResponse.class, response);
    final ErrorResponse errorResponse = (ErrorResponse) response;
    assertSame(XyzError.NOT_IMPLEMENTED, errorResponse.getError());
  }
}