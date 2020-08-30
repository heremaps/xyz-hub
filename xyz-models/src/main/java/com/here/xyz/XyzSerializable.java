/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz;

import static com.here.xyz.responses.XyzError.EXCEPTION;
import static com.here.xyz.responses.XyzError.TIMEOUT;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.LazyParsable.ProxyStringReader;
import com.here.xyz.models.hub.Space.Static;
import com.here.xyz.responses.ErrorResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import org.apache.commons.lang3.StringUtils;

public interface XyzSerializable {

  ThreadLocal<ObjectMapper> DEFAULT_MAPPER = ThreadLocal.withInitial(() -> new ObjectMapper().setSerializationInclusion(Include.NON_NULL));
  ThreadLocal<ObjectMapper> SORTED_MAPPER = ThreadLocal.withInitial(() ->
      new ObjectMapper().configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true).setSerializationInclusion(Include.NON_NULL));
  ThreadLocal<ObjectMapper> STATIC_MAPPER = ThreadLocal.withInitial(() -> new ObjectMapper().setConfig(
      DEFAULT_MAPPER.get().getSerializationConfig().withView(Static.class)));

  @SuppressWarnings("unused")
  static <T extends Typed> String serialize(T object) {
    try {
      return DEFAULT_MAPPER.get().writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unused")
  static String serialize(Object object, TypeReference typeReference) {
    try {
      return DEFAULT_MAPPER.get().writerFor(typeReference).writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  static <T extends Typed> T deserialize(InputStream is) throws JsonProcessingException {
    return (T) deserialize(is, Typed.class);
  }

  static <T> T deserialize(InputStream is, Class<T> klass) throws JsonProcessingException {
    try (Scanner scanner = new java.util.Scanner(is)) {
      return deserialize(scanner.useDelimiter("\\A").next(), klass);
    }
  }

  static <T extends Typed> T deserialize(String string) throws JsonProcessingException {
    //noinspection unchecked
    return (T) deserialize(string, Typed.class);
  }

  static <T> T deserialize(String string, Class<T> klass) throws JsonProcessingException {
    // Jackson always wraps larger strings, with a string reader, which hides the original string from the lazy raw deserializer.
    // To circumvent that wrap the source string with a custom string reader, which provides access to the input string.
    try {
      return DEFAULT_MAPPER.get().readValue(new ProxyStringReader(string), klass);
    } catch (JsonProcessingException e) {
      throw e;
    } catch (IOException e) {
      return null;
    }
  }

  @SuppressWarnings("unused")
  static <T> T deserialize(String string, TypeReference<T> type) throws JsonProcessingException {
    return DEFAULT_MAPPER.get().readerFor(type).readValue(string);
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

  @SuppressWarnings("unchecked")
  static <T extends XyzSerializable> T copy(T serializable) {
    try {
      return (T) XyzSerializable.deserialize(serializable.serialize(), serializable.getClass());
    } catch (Exception e) {
      return null;
    }
  }

  @SuppressWarnings("unused")
  static <T extends XyzSerializable> T fromMap(Map<String, Object> map, Class<T> klass) {
    return DEFAULT_MAPPER.get().convertValue(map, klass);
  }

  default String serialize() {
    return serialize(DEFAULT_MAPPER.get(), false);
  }

  @SuppressWarnings("unused")
  default String serialize(boolean pretty) {
    return serialize(DEFAULT_MAPPER.get(), pretty);
  }

  @SuppressWarnings("unused")
  default String serialize(ObjectMapper mapper) {
    return serialize(mapper, false);
  }

  default String serialize(ObjectMapper mapper, boolean pretty) {
    try {
      return pretty ? mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this) : mapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  default <T extends XyzSerializable> T copy() {
    try {
      //noinspection unchecked
      return (T) XyzSerializable.copy(this);

    } catch (Exception e) {
      return null;
    }
  }

  @SuppressWarnings("UnusedReturnValue")
  default Map<String, Object> asMap() {
    //noinspection unchecked
    return DEFAULT_MAPPER.get().convertValue(this, Map.class);
  }

  @SuppressWarnings("unused")
  default List<Object> asList() {
    //noinspection unchecked
    return DEFAULT_MAPPER.get().convertValue(this, List.class);
  }
}