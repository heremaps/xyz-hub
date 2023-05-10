package com.here.naksha.activitylog;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.IoEventPipeline;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.feature.GetFeaturesByIdEvent;
import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.IoHelp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

class ActivityLogHandlerTest {

  static Connector connector;
  static IoEventPipeline eventPipeline;
  static ActivityLogHandler activityLogHandler;

  @BeforeAll
  static void setup() throws XyzErrorException {
    connector = new Connector();
    eventPipeline = new IoEventPipeline();
    activityLogHandler = new ActivityLogHandler(connector);
    eventPipeline.addEventHandler(activityLogHandler);
  }

  @Test
  void test_GetFeaturesById() throws IOException {
    final GetFeaturesByIdEvent event = new GetFeaturesByIdEvent();
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    //XyzSerializable.deserialize(IoEventPipeline.readResource(""))
    eventPipeline.sendEvent(IoHelp.openResource("testevent.json"), out);
    final XyzResponse response = XyzSerializable.deserialize(out.toByteArray(), XyzResponse.class);
    assertNotNull(response);
    assertInstanceOf(ErrorResponse.class, response);
    final ErrorResponse errorResponse = (ErrorResponse) response;
    assertSame(XyzError.NOT_IMPLEMENTED, errorResponse.getError());
  }

  @Test
  void test_fromActivityLog() throws IOException {
    final Feature feature = XyzSerializable.deserialize(IoHelp.openResource("activity_log_feature.json"), Feature.class);
    assertNotNull(feature);
    activityLogHandler.fromActivityLogFormat(feature);
  }
}