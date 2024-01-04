/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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
package com.here.naksha.lib.handlers.util;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.fasterxml.jackson.core.type.TypeReference;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.view.ViewDeserialize;
import com.here.naksha.lib.core.view.ViewSerialize;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

public class MockUtil {

  public static String loadFile(final @NotNull String filePath) {
    try {
      return new String(ClassLoader.getSystemResourceAsStream(filePath).readAllBytes());
    } catch (IOException e) {
      throw unchecked(e);
    }
  }

  public static <T> T parseJson(final @NotNull String jsonStr, final @NotNull Class<T> type) {
    T obj = null;
    try (final Json json = Json.get()) {
      obj = json.reader(ViewDeserialize.Storage.class).forType(type).readValue(jsonStr);
    } catch (Exception ex) {
      throw unchecked(ex);
    }
    return obj;
  }

  public static <T> T parseJson(final @NotNull String jsonStr, final @NotNull TypeReference<T> type) {
    T obj = null;
    try (final Json json = Json.get()) {
      obj = json.reader(ViewDeserialize.Storage.class).forType(type).readValue(jsonStr);
    } catch (Exception ex) {
      throw unchecked(ex);
    }
    return obj;
  }

  public static String toJson(final @NotNull Object obj) {
    String jsonStr = null;
    try (final Json json = Json.get()) {
      jsonStr = json.writer(ViewSerialize.Storage.class).writeValueAsString(obj);
    } catch (Exception ex) {
      throw unchecked(ex);
    }
    return jsonStr;
  }

  public static <T> T parseJsonFile(final @NotNull String filePath, final @NotNull Class<T> type) {
    return parseJson(loadFile(filePath), type);
  }

  public static <T> T parseJsonFile(final @NotNull String filePath, final @NotNull TypeReference<T> type) {
    return parseJson(loadFile(filePath), type);
  }
}
