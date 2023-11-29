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
package com.here.naksha.app.common;

import static com.here.naksha.app.service.NakshaApp.newInstance;

import com.here.naksha.app.service.NakshaApp;

public class NakshaAppInitializer {

  private static final String MOCK_CONFIG_ID = "mock-config";

  private static final String LOCAL_TEST = "local-test-config";

  private NakshaAppInitializer() {}

  public static NakshaApp mockedNakshaApp() {
    return newInstance(MOCK_CONFIG_ID);
  }

  public static NakshaApp localPsqlBasedNakshaApp() {
    String dbUrl = System.getenv("TEST_NAKSHA_PSQL_URL");
    String password = System.getenv("TEST_NAKSHA_PSQL_PASS");
    if (password == null || password.isBlank()) {
      password = "password";
    }
    if (dbUrl == null || dbUrl.isBlank()) {
      dbUrl = "jdbc:postgresql://localhost/postgres?user=postgres&password=" + password
          + "&schema=naksha_test_maint_app";
    }

    return newInstance(LOCAL_TEST, dbUrl);
  }
}
