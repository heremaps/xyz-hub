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
package com.here.naksha.lib.core.util.json;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;
import static com.here.naksha.lib.core.models.XyzError.EXCEPTION;
import static com.here.naksha.lib.core.models.XyzError.TIMEOUT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.naksha.lib.core.LazyParsableFeatureList.ProxyStringReader;
import com.here.naksha.lib.core.models.Typed;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.core.view.ViewDeserialize.All;
import com.here.naksha.lib.core.view.ViewDeserialize.User;
import com.here.naksha.lib.core.view.ViewSerialize;
import java.io.IOException;
import java.io.InputStream;
import java.util.Formatter;
import java.util.Locale;
import java.util.Map;
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

  static String serialize(Object object) {
    try (final Json json = Json.get()) {
      return json.writer(ViewSerialize.User.class, false).writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw unchecked(e);
    }
  }

  static @NotNull String serialize(@Nullable Object object, @NotNull TypeReference<?> typeReference) {
    try (final Json json = Json.get()) {
      return json.writer(ViewSerialize.User.class, false)
          .forType(typeReference)
          .writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw unchecked(e);
    }
  }

  static <T extends Typed> @Nullable T deserialize(@NotNull InputStream is) {
    return (T) deserialize(is, Typed.class);
  }

  static <T> @Nullable T deserialize(@NotNull InputStream is, @NotNull Class<T> klass) {
    try (final Scanner scanner = new java.util.Scanner(is)) {
      return deserialize(scanner.useDelimiter("\\A").next(), klass);
    }
  }

  static <T> T deserialize(@NotNull InputStream is, @NotNull TypeReference<T> type) {
    try (final Json json = Json.get()) {
      return json.reader(User.class).forType(type).readValue(is);
    } catch (IOException e) {
      throw unchecked(e);
    }
  }

  static <T> T deserialize(byte @NotNull [] bytes, @NotNull TypeReference<T> type) {
    try (final Json json = Json.get()) {
      return json.reader(User.class).forType(type).readValue(bytes);
    } catch (IOException e) {
      throw unchecked(e);
    }
  }

  static <T extends Typed> @Nullable T deserialize(@NotNull String string) {
    return (T) deserialize(string, Typed.class);
  }

  static <T> @Nullable T deserialize(@NotNull String string, @NotNull Class<T> klass) {
    // Jackson always wraps larger strings, with a string reader, which hides the original string
    // from the lazy raw deserializer.
    // To circumvent that wrap the source string with a custom string reader, which provides access
    // to the input string.
    try (final Json json = Json.get()) {
      return json.reader(User.class).readValue(new ProxyStringReader(string), klass);
    } catch (final IOException e) {
      throw unchecked(e);
    }
  }

  @SuppressWarnings("unused")
  static <T> T deserialize(String string, TypeReference<T> type) {
    try (final Json json = Json.get()) {
      return json.reader(User.class).forType(type).readValue(string);
    } catch (IOException e) {
      throw unchecked(e);
    }
  }

  static <T> T deserialize(byte[] bytes, Class<T> klass) {
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
    return new ErrorResponse().withErrorMessage(errorMessage).withError(timeout ? TIMEOUT : EXCEPTION);
  }

  /**
   * Creates a deep clone of the given object by serialization and then deserialization.
   *
   * @param object   the object to clone.
   * @param <OBJECT> the object-type.
   * @return the clone.
   */
  static <OBJECT extends JsonSerializable> @NotNull OBJECT deepClone(@NotNull OBJECT object) {
    //noinspection ConstantConditions
    if (object == null) {
      return null;
    }
    try (final Json json = Json.get()) {
      final byte[] bytes = json.writer(ViewSerialize.All.class, false)
          .forType(object.getClass())
          .writeValueAsBytes(object);
      final Object clone =
          json.reader(All.class).forType(object.getClass()).readValue(bytes);
      return (OBJECT) clone;
    } catch (IOException e) {
      throw unchecked(e);
    }
  }

  @SuppressWarnings("unused")
  static <T extends JsonSerializable> T fromMap(Map<String, Object> map, Class<T> klass) {
    try (final Json json = Json.get()) {
      return json.mapper.convertValue(map, klass);
    }
  }

  static <T> @NotNull T fromAnyMap(@NotNull Map<@NotNull String, @Nullable Object> map, @NotNull Class<T> klass) {
    try (final Json json = Json.get()) {
      return json.mapper.convertValue(map, klass);
    }
  }

  default byte @NotNull [] toByteArray(@NotNull Class<? extends ViewSerialize> viewClass) {
    try (final Json json = Json.get()) {
      return json.writer(viewClass).writeValueAsBytes(this);
    } catch (JsonProcessingException e) {
      throw unchecked(e);
    }
  }

  default @NotNull String serialize() {
    return serialize(false);
  }

  default @NotNull String serialize(boolean pretty) {
    try (final Json json = Json.get()) {
      return json.writer(ViewSerialize.User.class, pretty).writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw unchecked(e);
    }
  }

  /**
   * Implementation of {@link Object#toString()} for all classes that simply want to serialize to JSON.
   *
   * @param object The object that should be serialized.
   * @return The serialize (JSON).
   */
  static @NotNull String toString(@Nullable Object object) {
    try (final Json json = Json.get()) {
      return json.writer(ViewSerialize.Internal.class, true).writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw unchecked(e);
    }
  }

  /**
   * Create a deep (recursive) clone of this object.
   *
   * @param <SELF> the own-type of this.
   * @return a clone of this object.
   */
  default <SELF extends JsonSerializable> @NotNull SELF deepClone() {
    return (SELF) JsonSerializable.deepClone(this);
  }

  default Map<String, Object> asMap() {
    try (final Json json = Json.get()) {
      return json.mapper.convertValue(this, Map.class);
    }
  }

  /**
   * Convert the given object into the target object. This normally returns a copy, but there is no
   * guarantee how much is copied.
   *
   * @param object      the object to convert.
   * @param targetClass the class of the target-type.
   * @param <TARGET>    the target-type.
   * @return the new instance converted.
   */
  static <TARGET extends JsonObject, OBJECT extends JsonObject> @NotNull TARGET convert(
      @NotNull OBJECT object, @NotNull Class<TARGET> targetClass) {
    try (final Json json = Json.get()) {
      return json.convert(object, targetClass);
    }
  }
}
