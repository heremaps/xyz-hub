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
package com.here.naksha.lib.hub;

import com.here.naksha.lib.psql.PsqlConfig;
import com.here.naksha.lib.psql.PsqlConfigBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

class NakshaHubTest {

  // TODO HP : Re-enable after NakshaHub initialization code starts working (dependency on psql module)
  // @BeforeAll
  static void prepare() {
    String password = System.getenv("TEST_NAKSHA_PSQL_PASS");
    if (password == null) password = "password";
    final PsqlConfig psqlCfg = new PsqlConfigBuilder()
        .withAppName(NakshaHubConfig.defaultAppName())
        .parseUrl("jdbc:postgresql://localhost/postgres?user=postgres&password=" + password
            + "&schema=naksha_test_hub")
        .build();
    hub = new NakshaHub(psqlCfg, null, null);
  }

  static NakshaHub hub;

  @Test
  void startup() throws InterruptedException {
    // TODO: test hub methods
  }

  @AfterAll
  static void close() throws InterruptedException {
    // TODO: Find a way to gracefully shutdown the hub
  }
}
