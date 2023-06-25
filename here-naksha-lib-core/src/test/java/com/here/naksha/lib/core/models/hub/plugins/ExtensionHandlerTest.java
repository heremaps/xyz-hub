package com.here.naksha.lib.core.models.hub.plugins;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.here.naksha.lib.core.EventPipeline;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.extension.ExtensionConfig;
import com.here.naksha.lib.core.extension.ExtensionHandler;
import com.here.naksha.lib.core.extension.NakshaExtSocket;
import com.here.naksha.lib.core.extension.messages.ExtensionMessage;
import com.here.naksha.lib.core.extension.messages.ProcessEventMsg;
import com.here.naksha.lib.core.extension.messages.ResponseMsg;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByIdEvent;
import com.here.naksha.lib.core.models.payload.responses.SuccessResponse;
import com.here.naksha.lib.core.util.json.Json;
import java.net.ServerSocket;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ExtensionHandlerTest {

  @BeforeAll
  static void init() {
    Json.DEBUG = true;
  }

  @AfterAll
  static void done() {
    Json.DEBUG = false;
  }

  static final int EXTENSION_ID = 2323;
  static final String CLASS_NAME = "com.here.some.Handler";

  static class SimulatedNakshaHub extends Thread {

    SimulatedNakshaHub() {}

    @Override
    public void run() {
      try {
        // Simulate what Naksha-Hub would do:
        final Connector testConnector = new Connector("test", CLASS_NAME);
        testConnector.setExtension(EXTENSION_ID);
        final ExtensionConfig config = new ExtensionConfig("http://localhost:" + EXTENSION_ID + "/");
        final IEventHandler eventHandler = new ExtensionHandler(testConnector, config);
        final EventPipeline eventPipeline = new EventPipeline();
        eventPipeline.addEventHandler(eventHandler);
        final GetFeaturesByIdEvent event = new GetFeaturesByIdEvent();
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
  void test_basics() throws Exception {
    try (final ServerSocket serverSocket = new ServerSocket(EXTENSION_ID); ) {
      // Start the Naksha-Hub.
      final SimulatedNakshaHub hub = new SimulatedNakshaHub();
      hub.start();

      // Accept incoming connection from the Naksha-Hub.
      try (final NakshaExtSocket nakshaSocket = new NakshaExtSocket(serverSocket.accept())) {

        // Wait for the message from the simulated Naksha-Hub.
        final ExtensionMessage msg = nakshaSocket.readMessage();
        assertNotNull(msg);
        // We should get an ProcessEvent envelope.
        final ProcessEventMsg processEvent = assertInstanceOf(ProcessEventMsg.class, msg);
        // The event in the envelope should be an GetFeaturesByIdEvent.
        assertNotNull(processEvent.event);
        assertInstanceOf(GetFeaturesByIdEvent.class, processEvent.event);
        // The connector being part of the envelope should have the expected class-name and extension ID.
        assertEquals(EXTENSION_ID, processEvent.connector.getExtension());
        assertEquals(CLASS_NAME, processEvent.connector.getClassName());

        // Simulate a SuccessResponse.
        final SuccessResponse response = new SuccessResponse();
        nakshaSocket.sendMessage(new ResponseMsg(response));

        // Wait until the simulated Naksha-Hub is done, and then verify the result.
        hub.join();
        assertNull(hub.exception);
        assertNotNull(hub.response);
        assertInstanceOf(SuccessResponse.class, hub.response);
      }
    }
  }
}
