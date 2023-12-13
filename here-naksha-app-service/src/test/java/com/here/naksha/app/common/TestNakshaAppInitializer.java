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
import static com.here.naksha.lib.psql.PsqlStorageConfig.configFromFileOrEnv;

import com.here.naksha.app.service.NakshaApp;
import com.here.naksha.lib.psql.PsqlStorageConfig;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class TestNakshaAppInitializer {

  public static final @NotNull PsqlStorageConfig adminDbConfig =
      configFromFileOrEnv("test_admin_db.url", "NAKSHA_TEST_ADMIN_DB_URL", "naksha_admin_schema");

  public static final @NotNull PsqlStorageConfig dataDbConfig =
      configFromFileOrEnv("test_data_db.url", "NAKSHA_TEST_DATA_DB_URL", "naksha_data_schema");

  private static final String MOCK_CONFIG_ID = "mock-config";

  private static final String TEST_CONFIG_ID = "test-config";

  public final @Nullable String testDbUrl;
  public final @NotNull AtomicReference<NakshaApp> nakshaApp;

  private TestNakshaAppInitializer(@Nullable String testDbUrl) {
    this.testDbUrl = testDbUrl;
    this.nakshaApp = new AtomicReference<>();
  }

  public NakshaApp initNaksha() {
    if (testDbUrl != null) {
      nakshaApp.compareAndSet(null, newInstance(TEST_CONFIG_ID, testDbUrl));
    } else {
      nakshaApp.compareAndSet(null, newInstance(MOCK_CONFIG_ID));
    }
    return nakshaApp.get();
  }

  // null if not initialized
  public @Nullable NakshaApp getNaksha() {
    return nakshaApp.get();
  }

  public static TestNakshaAppInitializer mockedNakshaApp() {
    return new TestNakshaAppInitializer(null);
  }

  public static TestNakshaAppInitializer localPsqlBasedNakshaApp() {
    return new TestNakshaAppInitializer(dataDbConfig.url());
  }
}
