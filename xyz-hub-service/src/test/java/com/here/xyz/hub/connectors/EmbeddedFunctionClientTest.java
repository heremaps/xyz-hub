/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.hub.connectors;

import static com.here.xyz.hub.connectors.EmbeddedFunctionClient.CORE_POOL_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.xyz.hub.Config;
import com.here.xyz.hub.Service;
import java.util.concurrent.ThreadPoolExecutor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class EmbeddedFunctionClientTest {

  private static Config previousConfiguration;

  @BeforeAll
  static void setUpAll() {
    previousConfiguration = Service.configuration;
    //RemoteFunctionClient reads the configuration when its class is initialized, baking the values into static finals.
    //Use non-zero values so that this test does not permanently poison those finals with zero, in case it is the one
    //that triggers that class' initialization first within the shared test JVM.
    Config config = new Config();
    config.GLOBAL_MAX_QUEUE_SIZE = 1; //MB
    config.REMOTE_FUNCTION_REQUEST_TIMEOUT = 10; //seconds
    Service.configuration = config;
  }

  @AfterAll
  static void tearDownAll() {
    Service.configuration = previousConfiguration;
  }

  //The maximum connections per node of a connector whose maxConnections is spread over many nodes can be below the core size
  @ParameterizedTest
  @ValueSource(ints = {1, 4, 7, 8, 9, 32, 256})
  void theExecutorAcceptsAnyMaximum(int maxConnections) {
    ThreadPoolExecutor executor = EmbeddedFunctionClient.newExecutor("test-connector", maxConnections);
    try {
      assertEquals(maxConnections, executor.getMaximumPoolSize());
      assertEquals(Math.min(CORE_POOL_SIZE, maxConnections), executor.getCorePoolSize());
    }
    finally {
      executor.shutdownNow();
    }
  }
}
