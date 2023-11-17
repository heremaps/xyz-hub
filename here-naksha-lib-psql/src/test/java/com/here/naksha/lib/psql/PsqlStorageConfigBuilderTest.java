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

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.naksha.lib.core.exceptions.ParameterError;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class PsqlStorageConfigBuilderTest {

  @Test
  void withUrl() throws URISyntaxException, ParameterError, UnsupportedEncodingException {
    PsqlStorageConfigBuilder builder = new PsqlStorageConfigBuilder();
    builder.parseUrl("jdbc:postgresql://localhost/test?user=foo&password=foobar&schema=bar");
    assertEquals("localhost", builder.getHost());
    assertEquals(5432, builder.getPort());
    assertEquals("test", builder.getDb());
    assertEquals("foo", builder.getUser());
    assertEquals("foobar", builder.getPassword());
    assertEquals("bar", builder.getSchema());

    builder = new PsqlStorageConfigBuilder();
    builder.parseUrl("jdbc:postgresql://foobar:1234/" + URLEncoder.encode("test:colon", StandardCharsets.UTF_8));
    assertEquals("foobar", builder.getHost());
    assertEquals(1234, builder.getPort());
    assertEquals("test:colon", builder.getDb());
    assertNull(builder.getUser());
    assertNull(builder.getPassword());
  }
}
