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
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import org.junit.jupiter.api.Test;

public class PsqlInstanceConfigTests {

  @Test
  void testUrlCreationWithCustomPort() {
    // given
    int port = 1234;
    PsqlInstanceConfig config = new PsqlInstanceConfig("localhost", port, "mydb", "user1", "pass1", null);

    // expect
    assertEquals("jdbc:postgresql://localhost:1234/mydb", config.url);
  }

  @Test
  void testUrlCreationDefaultPortAndReadonly() {
    // given
    Integer port = null;
    PsqlInstanceConfig config = new PsqlInstanceConfig("localhost", port, "mydb", "user1", "pass1", true);

    // expect
    assertEquals("jdbc:postgresql://localhost/mydb?readOnly=true", config.url);
  }

  @Test
  void testCreationWithInvalidPort() {
    // expect
    assertThrowsExactly(
        IllegalArgumentException.class,
        () -> new PsqlInstanceConfig("localhost", -1, "mydb", "user1", "pass1", null));
    assertThrowsExactly(
        IllegalArgumentException.class,
        () -> new PsqlInstanceConfig("localhost", 99999999, "mydb", "user1", "pass1", null));
  }

  @Test
  void testCreationWithInvalidHost() {
    // expect
    assertThrowsExactly(
        IllegalArgumentException.class, () -> new PsqlInstanceConfig("", null, "mydb", "user1", "pass1", null));
  }

  @Test
  void testCreationWithEmptyUser() {
    // expect
    assertThrowsExactly(
        IllegalArgumentException.class,
        () -> new PsqlInstanceConfig("localhost", null, "mydb", "", "pass1", null));
  }

  @Test
  void testCreationWithEmptyPassword() {
    // expect
    assertThrowsExactly(
        IllegalArgumentException.class,
        () -> new PsqlInstanceConfig("localhost", null, "mydb", "user1", "", null));
  }
}
