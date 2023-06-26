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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.storage.CollectionInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

public class PsqlStorageTest {

  /** This is mainly an example that you can use when running this test. */
  @SuppressWarnings("unused")
  public static final String TEST_ADMIN_DB =
      "jdbc:postgresql://localhost/postgres?user=postgres&password=password&schema=test";

  private boolean isAllTestEnvVarsSet() {
    return System.getenv("TEST_ADMIN_DB") != null
        && System.getenv("TEST_ADMIN_DB").length() > "jdbc:postgresql://".length();
  }

  static NakshaVersion backup;

  @BeforeAll
  static void beforeAll() {
    backup = PsqlStorage.latest;
    PsqlStorage.latest = new NakshaVersion(999, 0, 0);
  }

  @AfterAll
  static void afterAll() {
    PsqlStorage.latest = backup;
  }

  @Test
  @EnabledIf("isAllTestEnvVarsSet")
  public void testInit() throws Exception {
    final PsqlConfig config = new PsqlConfigBuilder()
        .withAppName("Naksha-Psql-Test")
        .parseUrl(System.getenv("TEST_ADMIN_DB"))
        .build();
    try (final var client = new PsqlStorage(config, 0L)) {
      client.init();
      try (final var tx = client.openMasterTransaction()) {
        final CollectionInfo foo = tx.createCollection(new CollectionInfo("foo"));
        assertNotNull(foo);
        assertEquals("foo", foo.getId());
        assertEquals(9223372036854775807L, foo.getMaxAge());
        assertTrue(foo.getHistory());
        tx.commit();
      }
    }
  }
}
