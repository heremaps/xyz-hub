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
package com.here.naksha.app.service;

import static com.here.naksha.app.service.NakshaApp.newHub;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

class NakshaHubTest {

  // TODO HP : Re-enable test after fixing the Naksha service
  // @BeforeAll
  static void prepare() {
    String password = System.getenv("TEST_NAKSHA_PSQL_PASS");
    if (password == null) password = "password";
    hub = newHub(
        "jdbc:postgresql://localhost/postgres?user=postgres&password=" + password,
        "default-config"); // this string concat only happens once, its ok ;)
    hub.start();
  }

  static NakshaApp hub;

  @Test
  void startup() throws InterruptedException {
    // curl http://localhost:8080/
    // TODO: Send some test request!
  }

  @AfterAll
  static void close() throws InterruptedException {
    // TODO: Find a way to gracefully shutdown the server.
    //       To do some manual testing with the running service, uncomment this:
    // hub.join(java.util.concurrent.TimeUnit.SECONDS.toMillis(60));
  }
}
