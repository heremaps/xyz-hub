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

package com.here.xyz.hub.task;

import static com.here.xyz.hub.task.IcebergReadMode.OFF;
import static com.here.xyz.hub.task.IcebergReadMode.ON;
import static com.here.xyz.hub.task.IcebergReadMode.SHADOW;
import static org.assertj.core.api.Assertions.assertThat;

import com.here.xyz.hub.Config;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.models.Space;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class IcebergReadModeTest {

  private Config previousConfiguration;

  @BeforeEach
  void setUp() {
    previousConfiguration = Service.configuration;
    Service.configuration = new Config();
  }

  @AfterEach
  void tearDown() {
    Service.configuration = previousConfiguration;
  }

  static Stream<Arguments> globalAndSpaceValues() {
    //global, space value, effective mode
    return Stream.of(
        Arguments.of("OFF", null, OFF),
        Arguments.of("OFF", "OFF", OFF),
        Arguments.of("OFF", "ON", ON),
        Arguments.of("OFF", "SHADOW", SHADOW),
        Arguments.of("ON", null, ON),
        Arguments.of("ON", "OFF", OFF),
        Arguments.of("ON", "ON", ON),
        Arguments.of("ON", "SHADOW", SHADOW));
  }

  @ParameterizedTest(name = "global {0}, space {1}")
  @MethodSource("globalAndSpaceValues")
  void theSpaceValueTakesPrecedence(String global, String spaceValue, IcebergReadMode expected) {
    Service.configuration.ICEBERG_READS = global;
    assertThat(IcebergReadMode.of(space(spaceValue))).isEqualTo(expected);
  }

  @Test
  void theGlobalSettingIsOffByDefault() {
    assertThat(IcebergReadMode.global()).isEqualTo(OFF);
    assertThat(IcebergReadMode.of(space(null))).isEqualTo(OFF);
    assertThat(IcebergReadMode.of(null)).isEqualTo(OFF);
    Service.configuration.ICEBERG_READS = null;
    assertThat(IcebergReadMode.global()).isEqualTo(OFF);
    Service.configuration.ICEBERG_READS = " ";
    assertThat(IcebergReadMode.global()).isEqualTo(OFF);
  }

  @Test
  void theGlobalSettingIgnoresTheCase() {
    Service.configuration.ICEBERG_READS = " on ";
    assertThat(IcebergReadMode.global()).isEqualTo(ON);
    Service.configuration.ICEBERG_READS = "Off";
    assertThat(IcebergReadMode.global()).isEqualTo(OFF);
  }

  @Test
  void shadowAndUnknownGlobalValuesAreOff() {
    for (String value : new String[]{"SHADOW", "shadow", "unknown", "TRUE"}) {
      Service.configuration.ICEBERG_READS = value;
      assertThat(IcebergReadMode.global()).as(value).isEqualTo(OFF);
      assertThat(IcebergReadMode.of(space(null))).as(value).isEqualTo(OFF);
      //A space value still applies
      assertThat(IcebergReadMode.of(space("ON"))).as(value).isEqualTo(ON);
      assertThat(IcebergReadMode.of(space("SHADOW"))).as(value).isEqualTo(SHADOW);
    }
  }

  @Test
  void anUnknownSpaceValueIsOff() {
    Service.configuration.ICEBERG_READS = "ON";
    assertThat(IcebergReadMode.of(space("unknown"))).isEqualTo(OFF);
    assertThat(IcebergReadMode.of(space(""))).isEqualTo(OFF);
  }

  @Test
  void aCompositeHasItsOwnMode() {
    Service.configuration.ICEBERG_READS = "OFF";
    Space composite = IcebergReadTestSupport.composite(5L);
    //The base is not looked at
    assertThat(IcebergReadMode.of(composite)).isEqualTo(OFF);
    composite.setIcebergReads("ON");
    assertThat(IcebergReadMode.of(composite)).isEqualTo(ON);
  }

  @Test
  void onlyTheExactNamesAreValidSpaceValues() {
    assertThat(IcebergReadMode.isValidSpaceValue(null)).isTrue();
    assertThat(Arrays.stream(IcebergReadMode.values()).map(Enum::name).allMatch(IcebergReadMode::isValidSpaceValue)).isTrue();
    for (String value : new String[]{"", "on", " ON", "unknown", "true"})
      assertThat(IcebergReadMode.isValidSpaceValue(value)).as(value).isFalse();
  }

  private static Space space(String icebergReads) {
    Space space = IcebergReadTestSupport.space(IcebergReadTestSupport.SPACE_ID);
    space.setIcebergReads(icebergReads);
    return space;
  }
}
