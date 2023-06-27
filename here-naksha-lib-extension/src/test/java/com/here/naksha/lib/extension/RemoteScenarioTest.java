/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */
package com.here.naksha.lib.extension;

import com.here.naksha.lib.core.EventPipeline;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.extension.ExtensionHandler;
import com.here.naksha.lib.core.models.features.Connector;
import com.here.naksha.lib.core.models.features.Extension;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.events.info.HealthCheckEvent;
import com.here.naksha.lib.core.models.payload.responses.HealthStatus;
import com.here.naksha.lib.core.util.json.Json;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class RemoteScenarioTest {

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
        final Connector testConnector = new Connector("test", "com.here.naksha.ext.handler.ExampleJavaHandler");
        testConnector.setExtension(EXTENSION_ID);
        final Extension config = new Extension("localhost", EXTENSION_ID);
        final IEventHandler eventHandler = new ExtensionHandler(testConnector, config);
        final EventPipeline eventPipeline = new EventPipeline();
        eventPipeline.addEventHandler(eventHandler);
        final HealthCheckEvent event = new HealthCheckEvent();
        //        event.setConnector(testConnector);
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

    // final RemoteExtensionServer remoteExtensionServer = new RemoteExtensionServer(EXTENSION_ID);
    hub.join();
    Assertions.assertInstanceOf(HealthStatus.class, hub.response);
  }
}
