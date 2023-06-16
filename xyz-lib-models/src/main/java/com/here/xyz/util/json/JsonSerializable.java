/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

package com.here.xyz.util.json;

import static com.here.xyz.responses.XyzError.EXCEPTION;
import static com.here.xyz.responses.XyzError.TIMEOUT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.LazyParsableFeatureList.ProxyStringReader;
import com.here.xyz.Typed;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.view.Deserialize;
import com.here.xyz.view.Serialize;
import java.io.IOException;
import java.io.InputStream;
import java.util.Formatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * All objects being serializable should implement this interface.
 */
@SuppressWarnings({"unchecked", "UnusedReturnValue"})
public interface JsonSerializable {

  /**
   * Format a string using the {@link Formatter}.
   *
   * @param format The format string.
   * @param args   The arguments.
   * @return The formatted string.
   */
  static @NotNull String format(@NotNull String format, Object... args) {
    return String.format(Locale.US, format, args);
  }

  static <T extends Typed> String serialize(T object) {
    try (final Json json = Json.open()) {
      return json.writer(Serialize.Public.class, false).writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  static @NotNull String serialize(@Nullable Object object, @NotNull TypeReference<?> typeReference) {
    try (final Json json = Json.open()) {
      return json.writer(Serialize.Public.class, false).forType(typeReference).writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  static <T extends Typed> @Nullable T deserialize(@NotNull InputStream is) throws JsonProcessingException {
    return (T) deserialize(is, Typed.class);
  }

  static <T> @Nullable T deserialize(@NotNull InputStream is, @NotNull Class<T> klass) throws JsonProcessingException {
    try (final Scanner scanner = new java.util.Scanner(is)) {
      return deserialize(scanner.useDelimiter("\\A").next(), klass);
    }
  }

  static <T> T deserialize(@NotNull InputStream is, @NotNull TypeReference<T> type) throws IOException {
    try (final Json json = Json.open()) {
      return json.reader(Deserialize.Public.class).forType(type).readValue(is);
    }
  }

  static <T> T deserialize(byte @NotNull[] bytes, @NotNull TypeReference<T> type) throws IOException {
    try (final Json json = Json.open()) {
      return json.reader(Deserialize.Public.class).forType(type).readValue(bytes);
    }
  }

  static <T extends Typed> @Nullable T deserialize(@NotNull String string) throws JsonProcessingException {
    return (T) deserialize(string, Typed.class);
  }

  static <T> @Nullable T deserialize(@NotNull String string, @NotNull Class<T> klass) throws JsonProcessingException {
    // Jackson always wraps larger strings, with a string reader, which hides the original string from the lazy raw deserializer.
    // To circumvent that wrap the source string with a custom string reader, which provides access to the input string.
    try (final Json json = Json.open()) {
      return json.reader(Deserialize.Public.class).readValue(new ProxyStringReader(string), klass);
    } catch (IOException e) {
      throw new JsonProcessingIoException(e);
    }
  }

  @SuppressWarnings("unused")
  static <T> T deserialize(String string, TypeReference<T> type) throws JsonProcessingException {
    try (final Json json = Json.open()) {
      return json.reader(Deserialize.Public.class).forType(type).readValue(string);
    }
  }

  static <T> T deserialize(byte[] bytes, Class<T> klass) throws JsonProcessingException {
    return deserialize(new String(bytes), klass);
  }

  static ErrorResponse fixAWSLambdaResponse(final ErrorResponse errorResponse) {
    if (errorResponse == null) {
      return null;
    }

    final String errorMessage = errorResponse.getErrorMessage();
    if (StringUtils.isEmpty(errorMessage)) {
      return errorResponse;
    }

    boolean timeout = errorMessage.contains("timed out");
    return new ErrorResponse()
        .withErrorMessage(errorMessage)
        .withError(timeout ? TIMEOUT : EXCEPTION);
  }

  static <T extends JsonSerializable> @NotNull T copy(@NotNull T serializable) {
    try {
      return (T) Objects.requireNonNull(JsonSerializable.deserialize(serializable.serialize(), serializable.getClass()));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unused")
  static <T extends JsonSerializable> T fromMap(Map<String, Object> map, Class<T> klass) {
    try (final Json json = Json.open()) {
      return json.mapper.convertValue(map, klass);
    }
  }

  static <T> @NotNull T fromAnyMap(@NotNull Map<@NotNull String, @Nullable Object> map, @NotNull Class<T> klass) {
    try (final Json json = Json.open()) {
      return json.mapper.convertValue(map, klass);
    }
  }

  default byte @NotNull [] toByteArray() {
    return serialize().getBytes();
  }

  default @NotNull String serialize() {
    return serialize(false);
  }

  default @NotNull String serialize(boolean pretty) {
    try (final Json json = Json.open()) {
      return json.writer(Serialize.Public.class, pretty).writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  default <T extends JsonSerializable> @NotNull T copy() {
    return (T) JsonSerializable.copy(this);
  }

  default Map<String, Object> asMap() {
    try (final Json json = Json.open()) {
      return json.mapper.convertValue(this, Map.class);
    }
  }

  default List<Object> asList() {
    try (final Json json = Json.open()) {
      return json.mapper.convertValue(this, List.class);
    }
  }
}