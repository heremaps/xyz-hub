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

package com.here.xyz;

import static com.here.xyz.XyzSerializable.Mappers.getDefaultMapper;
import static com.here.xyz.XyzSerializable.Mappers.getMapperForView;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.LazyParsable.ProxyStringReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public interface XyzSerializable {

  class Mappers {
    public static final ThreadLocal<ObjectMapper> DEFAULT_MAPPER = ThreadLocal.withInitial(
        () -> registerNewMapper(new ObjectMapper().setSerializationInclusion(Include.NON_NULL)));
    private static final ThreadLocal<ObjectMapper> STATIC_MAPPER = ThreadLocal.withInitial(
        () -> registerNewMapper(new ObjectMapper().setConfig(DEFAULT_MAPPER.get().getSerializationConfig().withView(Static.class))));
    protected static final ThreadLocal<ObjectMapper> SORTED_MAPPER = ThreadLocal.withInitial(
        () -> registerNewMapper(new ObjectMapper().configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
            .setSerializationInclusion(Include.NON_NULL)));
    private static final Collection<Class<?>> REGISTERED_SUBTYPES = new ConcurrentLinkedQueue<>();
    private static final Collection<ObjectMapper> ALL_MAPPERS = Collections.newSetFromMap(Collections.synchronizedMap(new WeakHashMap<>()));

    private static ObjectMapper registerNewMapper(ObjectMapper om) {
      ALL_MAPPERS.add(om);
      om.registerSubtypes(REGISTERED_SUBTYPES.toArray(Class<?>[]::new));
      return om;
    }

    private static void registerSubtypes(Class<?>... classes) {
      //Add the new subtypes to the list of registered subtypes so that they will be registered on new ObjectMappers
      REGISTERED_SUBTYPES.addAll(Arrays.asList(classes));
      //Register the new subtypes on all existing mappers
      ALL_MAPPERS.forEach(om -> om.registerSubtypes(classes));
    }

    protected static ObjectMapper getMapperForView(Class<? extends SerializationView> view) {
      if (Static.class.isAssignableFrom(view))
        return STATIC_MAPPER.get();
      else
        return getDefaultMapper();
    }

    protected static ObjectMapper getDefaultMapper() {
      return DEFAULT_MAPPER.get();
    }
  }

  /**
   * Can be used to register additional subtypes at runtime for types which will be deserialized polymorphically.
   * The subtypes will be registered on all newly created {@link ObjectMapper}s as well as on all existing ones immediately.
   * @param classes The classes to register for deserialization purposes
   */
  static void registerSubtypes(Class<?>... classes) {
    Mappers.registerSubtypes(classes);
  }

  default String serialize() {
    return serialize(this);
  }

  default String serialize(Class<? extends SerializationView> view) {
    return serialize(this, view);
  }

  @SuppressWarnings("unused")
  default String serialize(boolean pretty) {
    return serialize(this, Public.class, pretty);
  }

  @SuppressWarnings("unused")
  static String serialize(Object object) {
    return serialize(object, Public.class, false);
  }

  static String serialize(Object object, Class<? extends SerializationView> view) {
    return serialize(object, view, false);
  }

  private static String serialize(Object object, Class<? extends SerializationView> view, boolean pretty) {
    ObjectMapper mapper = getMapperForView(view);
    try {
      return pretty ? mapper.writerWithDefaultPrettyPrinter().writeValueAsString(object) : mapper.writeValueAsString(object);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to encode as JSON: " + e.getMessage(), e);
    }
  }

  /**
   * @deprecated Please use {@link #serialize(Object)} instead.
   * @param object
   * @param typeReference
   * @return
   */
  @Deprecated
  @SuppressWarnings("unused")
  static String serialize(Object object, TypeReference typeReference) {
    try {
      return getDefaultMapper().writerFor(typeReference).writeValueAsString(object);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  default byte[] toByteArray() {
    return toByteArray(this);
  }

  default byte[] toByteArray(Class<? extends SerializationView> view) {
    return toByteArray(this, view);
  }

  static byte[] toByteArray(Object object) {
    return toByteArray(object, Public.class);
  }

  static byte[] toByteArray(Object object, Class<? extends SerializationView> view) {
    return serialize(object, view).getBytes();
  }

  @SuppressWarnings("UnusedReturnValue")
  default Map<String, Object> toMap() {
    return toMap(this);
  }

  default Map<String, Object> toMap(Class<? extends SerializationView> view) {
    //noinspection unchecked
    return toMap(this, view);
  }

  static Map<String, Object> toMap(Object object) {
    return toMap(object, Public.class);
  }

  static Map<String, Object> toMap(Object object, Class<? extends SerializationView> view) {
    return getMapperForView(view).convertValue(object, Map.class);
  }

  @SuppressWarnings("unused")
  default List<Object> toList() {
    return toList(this);
  }

  default List<Object> toList(Class<? extends SerializationView> view) {
    //noinspection unchecked
    return toList(this, view);
  }

  static List<Object> toList(Object object) {
    return toList(object, Public.class);
  }

  static List<Object> toList(Object object, Class<? extends SerializationView> view) {
    return getMapperForView(view).convertValue(object, List.class);
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

  static <T> T deserialize(InputStream is, TypeReference<T> type) throws JsonProcessingException {
    try (Scanner scanner = new java.util.Scanner(is)) {
      return deserialize(scanner.useDelimiter("\\A").next(), type);
    }
  }

  static <T extends Typed> T deserialize(byte[] bytes) throws JsonProcessingException {
    return (T) deserialize(bytes, Typed.class);
  }

  static <T> T deserialize(byte[] bytes, Class<T> klass) throws JsonProcessingException {
    return deserialize(new String(bytes), klass);
  }

  static <T> T deserialize(byte[] bytes, TypeReference<T> type) throws JsonProcessingException {
    return deserialize(new String(bytes), type);
  }

  static <T extends Typed> T deserialize(String string) throws JsonProcessingException {
    //noinspection unchecked
    return (T) deserialize(string, Typed.class);
  }

  static <T> T deserialize(String string, Class<T> klass) throws JsonProcessingException {
    /*
    Jackson always wraps larger strings, with a string reader, which hides the original string from the lazy raw deserializer.
    To circumvent that wrap the source string with a custom string reader, which provides access to the input string.
     */
    try {
      return getDefaultMapper().readValue(new ProxyStringReader(string), klass);
    }
    catch (JsonProcessingException e) {
      //NOTE: This catch block must stay, because JsonProcessingException extends IOException
      throw e;
    }
    catch (IOException e) {
      return null;
    }
  }

  @SuppressWarnings("unused")
  static <T> T deserialize(String string, TypeReference<T> type) throws JsonProcessingException {
    return getDefaultMapper().readerFor(type).readValue(string);
  }

  static <T extends Typed> T fromMap(Map<String, Object> map) {
    return (T) fromMap(map, Typed.class);
  }

  @SuppressWarnings("unused")
  static <T> T fromMap(Map<String, Object> map, Class<T> klass) {
    return getDefaultMapper().convertValue(map, klass);
  }

  static <T> T fromMap(Map<String, Object> map, TypeReference<T> type) {
    return getDefaultMapper().convertValue(map, type);
  }

  @SuppressWarnings("unchecked")
  default <T extends XyzSerializable> T copy() {
    try {
      //noinspection unchecked
      return (T) XyzSerializable.deserialize(serialize(), getClass());
    }
    catch (Exception e) {
      return null;
    }
  }

  interface SerializationView {}

  /**
   * Used as a JsonView on {@link Payload} models to indicate that a property should be serialized as part of public responses.
   * (e.g. when it comes to REST API responses)
   */
  @SuppressWarnings("WeakerAccess")
  class Public implements SerializationView {}

  /**
   * Used as a JsonView on {@link Payload} models to indicate that a property should be serialized in the persistence layer.
   * (e.g. when it comes to saving the instance to a database)
   */
  class Static implements SerializationView {}
}