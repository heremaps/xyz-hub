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
package com.here.naksha.lib.core.util;

import static com.here.naksha.lib.core.util.IoHelp.readConfigFromHomeOrResource;
import static org.junit.jupiter.api.Assertions.*;

import com.here.naksha.lib.core.util.IoHelp.LoadedConfig;
import java.io.IOException;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

class IoHelpTest {

  // This app name should not exists.
  private static final String APP_NAME = RandomStringUtils.random(40);

  public static class ConfigTest {

    public int theInt;
    public boolean theBool;
    public String theString;
    public String env;
  }

  @Test
  void test_configFileFromResources() throws IOException {
    final LoadedConfig<ConfigTest> loadedConfig =
        readConfigFromHomeOrResource("iohelp_config_test.json", false, APP_NAME, ConfigTest.class);
    assertNotNull(loadedConfig);
    final ConfigTest config = loadedConfig.config();
    assertNotNull(config);
    assertEquals(100, config.theInt);
    assertTrue(config.theBool);
    assertEquals("Hello World!", config.theString);
    assertEquals(config.env, "default");
  }
}
