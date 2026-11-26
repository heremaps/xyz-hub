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

package com.here.xyz.jobs.steps.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.here.xyz.XyzSerializable;
import com.here.xyz.XyzSerializable.Static;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.util.service.BaseConfig;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RunEmrJobCloudWatchLoggingTest {
  private static final String LOG_GROUP_NAME = "/test/log-group";
  private static final Map<String, List<String>> DRIVER_LOG_TYPES = Map.of("SPARK_DRIVER", List.of("STDERR", "STDOUT"));
  private Config previousConfig;
  private BaseConfig previousBaseConfig;

  @BeforeEach
  void setUp() {
    previousConfig = Config.instance;
    //new Config() replaces BaseConfig.instance as well
    previousBaseConfig = BaseConfig.instance;
    new Config();
  }

  @AfterEach
  void tearDown() {
    Config.instance = previousConfig;
    BaseConfig.instance = previousBaseConfig;
  }

  @Test
  void loggingConfigurationIsKeptWhenStoredAndSerialized() throws Exception {
    RunEmrJob step = new RunEmrJob()
        .withCloudWatchLogGroupName(LOG_GROUP_NAME)
        .withCloudWatchLogTypes(DRIVER_LOG_TYPES);

    RunEmrJob storedStep = XyzSerializable.deserialize(step.serialize(Static.class), RunEmrJob.class);
    assertEquals(LOG_GROUP_NAME, storedStep.getCloudWatchLogGroupName());
    assertEquals(DRIVER_LOG_TYPES, storedStep.getCloudWatchLogTypes());

    RunEmrJob serializedStep = XyzSerializable.deserialize(step.serialize(), RunEmrJob.class);
    assertEquals(LOG_GROUP_NAME, serializedStep.getCloudWatchLogGroupName());
    assertEquals(DRIVER_LOG_TYPES, serializedStep.getCloudWatchLogTypes());
  }

  @Test
  void stepStoredWithoutLoggingConfigurationHasNone() throws Exception {
    String storedJson = new RunEmrJob().serialize(Static.class);
    assertFalse(storedJson.contains("cloudWatchLogGroupName"));
    assertFalse(storedJson.contains("cloudWatchLogTypes"));

    RunEmrJob storedStep = XyzSerializable.deserialize(storedJson, RunEmrJob.class);
    assertNull(storedStep.getCloudWatchLogGroupName());
    assertNull(storedStep.getCloudWatchLogTypes());
  }
}
