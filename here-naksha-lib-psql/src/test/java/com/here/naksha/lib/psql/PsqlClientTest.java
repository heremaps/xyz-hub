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
package com.here.naksha.lib.psql;

import com.here.naksha.lib.core.storage.IMasterTransaction;
import com.here.naksha.lib.core.storage.IStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

public class PsqlClientTest {

  /** This is mainly an example that you can use when running this test. */
  @SuppressWarnings("unused")
  public static final String TEST_ADMIN_DB =
      "jdbc:postgresql://localhost/postgres?user=postgres&password=password&schema=test";

  private boolean isAllTestEnvVarsSet() {
    return System.getenv("TEST_ADMIN_DB") != null
        && System.getenv("TEST_ADMIN_DB").length() > "jdbc:postgresql://".length();
  }

  @Test
  @EnabledIf("isAllTestEnvVarsSet")
  public void testInit() throws Exception {
    final PsqlConfig config = new PsqlConfigBuilder()
        .withAppName("Naksha-Psql-Test")
        .parseUrl(System.getenv("TEST_ADMIN_DB"))
        .build();
    try (final IStorage client = new PsqlStorage(config, 0L)) {
      try (final IMasterTransaction tx = client.openMasterTransaction()) {}
    }
  }
}
