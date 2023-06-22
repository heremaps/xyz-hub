package com.here.naksha.lib.core.models.hub.plugins;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.here.naksha.lib.core.EventPipeline;
import com.here.naksha.lib.core.IEventContext;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.extension.ExtensionConfig;
import com.here.naksha.lib.core.extension.ExtensionHandler;
import com.here.naksha.lib.core.extension.NakshaExtSocket;
import com.here.naksha.lib.core.extension.messages.ExtensionMessage;
import com.here.naksha.lib.core.extension.messages.ProcessEvent;
import com.here.naksha.lib.core.extension.messages.ReturnResponse;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByIdEvent;
import com.here.naksha.lib.core.models.payload.responses.SuccessResponse;
import com.here.naksha.lib.core.util.json.Json;
import java.net.ServerSocket;
import java.net.Socket;
import org.jetbrains.annotations.NotNull;
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

    static class TestHandler implements IEventHandler {

        @Override
        public @NotNull XyzResponse processEvent(@NotNull IEventContext eventContext) throws XyzErrorException {
            return new SuccessResponse();
        }
    }

    static class SimulatedNakshaHub extends Thread {

        SimulatedNakshaHub() {}

        @Override
        public void run() {
            try {
                // Simulate what Naksha-Hub would do:
                final Connector testConnector = new Connector("test", TestHandler.class);
                testConnector.setExtension(EXTENSION_ID);
                final ExtensionConfig config = new ExtensionConfig("http://localhost:" + EXTENSION_ID + "/");
                final IEventHandler eventHandler = new ExtensionHandler(testConnector, config);
                final EventPipeline eventPipeline = new EventPipeline();
                eventPipeline.addEventHandler(eventHandler); // We expect, that this adds the extension handler
                final GetFeaturesByIdEvent event = new GetFeaturesByIdEvent();
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
            final Socket socket = serverSocket.accept();
            try (final NakshaExtSocket nakshaSocket = new NakshaExtSocket(socket)) {

                // The message should be an ProcessEvent.
                final ExtensionMessage msg = nakshaSocket.readMessage();
                assertNotNull(msg);
                final ProcessEvent processEvent = assertInstanceOf(ProcessEvent.class, msg);
                assertEquals(EXTENSION_ID, processEvent.connector.getExtension());
                assertNotNull(processEvent.event);
                assertInstanceOf(GetFeaturesByIdEvent.class, processEvent.event);

                // Send back a response.
                final SuccessResponse response = new SuccessResponse();
                nakshaSocket.sendMessage(new ReturnResponse(response));

                // Wait until the hub is done and then verify the result.
                hub.join();
                assertNull(hub.exception);
                assertNotNull(hub.response);

                assertInstanceOf(SuccessResponse.class, hub.response);
            }
        }
    }
}
