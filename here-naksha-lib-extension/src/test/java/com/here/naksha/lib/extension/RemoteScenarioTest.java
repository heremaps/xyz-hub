package com.here.naksha.lib.extension;

import com.here.naksha.lib.core.EventPipeline;
import com.here.naksha.lib.core.IEventContext;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.extension.ExtensionConfig;
import com.here.naksha.lib.core.extension.ExtensionHandler;
import com.here.naksha.lib.core.extension.messages.ReturnResponse;
import com.here.naksha.lib.core.models.hub.plugins.Connector;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.events.info.HealthCheckEvent;
import com.here.naksha.lib.core.models.payload.responses.HealthStatus;
import com.here.naksha.lib.core.util.json.Json;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class RemoteScenarioTest {
 /** A handler that the customer supplied and wish to deploy. */
  public class RemoteCustomerHandler implements IEventHandler {

    @Override
    public @NotNull XyzResponse processEvent(@NotNull IEventContext eventContext) {
      return new HealthStatus();
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

  static final int EXTENSION_ID = 2323;

  static class SimulatedNakshaHub extends Thread {

    SimulatedNakshaHub() {}

    @Override
    public void run() {
      try {
        // Simulate what Naksha-Hub would do:
        final Connector testConnector = new Connector("test", RemoteCustomerHandler.class);
        testConnector.setExtension(EXTENSION_ID);
        final ExtensionConfig config = new ExtensionConfig("http://localhost:" + EXTENSION_ID + "/");
        final IEventHandler eventHandler = new ExtensionHandler(testConnector, config);
        final EventPipeline eventPipeline = new EventPipeline();
        eventPipeline.addEventHandler(eventHandler);
        final HealthCheckEvent event = new HealthCheckEvent();
        event.setConnector(testConnector);
        // This sends the event through the pipeline.
        // The extension handler will serialize the event and send it to our server, run by the test.
        // This server simulates the CLASS_NAME class and returns something, we remember what is returned.
        response = eventPipeline.sendEvent(event);
      } catch (Exception e) {
        exception = e;
      }
    }

    XyzResponse response;
    Exception exception;
  }


  @Test
  public void testRemote() throws Exception {
    final SimulatedNakshaHub hub = new SimulatedNakshaHub();
    hub.start();

    final RemoteExtensionServer remoteExtensionServer = new RemoteExtensionServer(EXTENSION_ID);
    hub.join();
    Assertions.assertInstanceOf(ReturnResponse.class,hub.response);
  }
}
