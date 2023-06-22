package com.here.naksha.lib.core.models.hub.plugins;

import com.here.naksha.lib.core.EventPipeline;
import com.here.naksha.lib.core.IEventContext;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByIdEvent;
import com.here.naksha.lib.core.util.json.Json;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ExtensionHandlerTest {

  private static class TestHandler implements IEventHandler {

    @Override
    public @NotNull XyzResponse processEvent(@NotNull IEventContext eventContext) throws XyzErrorException {
      return null;
    }
  }

  @BeforeAll
  static void init() {
    Json.DEBUG = true;
  }

  @AfterAll
  static void done() {
    Json.DEBUG = false;
  }

  @Test
  void test_basics() throws Exception {
    final Connector testConnector = new Connector("test", TestHandler.class);
    testConnector.setExtension(1000);
    final IEventHandler eventHandler = new ExtensionHandler(testConnector, new ExtensionConfig("http://localhost:1000/"));
    final EventPipeline eventPipeline = new EventPipeline();
    eventPipeline.addEventHandler(eventHandler);
    final GetFeaturesByIdEvent event = new GetFeaturesByIdEvent();
    // TODO: We need a test web-server to really do the test.
    //eventPipeline.sendEvent(event);
  }
}