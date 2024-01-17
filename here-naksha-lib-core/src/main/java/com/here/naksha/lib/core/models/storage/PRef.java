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
package com.here.naksha.lib.core.models.storage;

import static com.here.naksha.lib.core.util.StringCache.string;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The property references, basically addresses all properties that can be searched.
 */
public class PRef {

  /**
   * Create a references to an arbitrary property at the given JSON path.
   *
   * @param path The path to the property.
   */
  PRef(@NotNull String... path) {
    this.propertyPath = List.of(path);
  }

  @NotNull
  PRef withTagName(@NotNull String tagName) {
    this.tagName = tagName;
    return this;
  }

  public static final String[] ID_PROP_PATH = new String[] {"id"};
  public static final String[] APP_ID_PROP_PATH = new String[] {"properties", "@ns:com:here:xyz", "app_id"};
  public static final String[] AUTHOR_PROP_PATH = new String[] {"properties", "@ns:com:here:xyz", "author"};
  public static final String[] UUID_PROP_PATH = new String[] {"properties", "@ns:com:here:xyz", "uuid"};
  public static final String[] MRID_PROP_PATH = new String[] {"properties", "@ns:com:here:xyz", "mrid"};
  public static final String[] GRID_PROP_PATH = new String[] {"properties", "@ns:com:here:xyz", "qrid"};
  public static final String[] TXN_PROP_PATH = new String[] {"properties", "@ns:com:here:xyz", "txn"};
  public static final String[] TXN_NEXT_PROP_PATH = new String[] {"properties", "@ns:com:here:xyz", "txn_next"};
  public static final String[] TAGS_PROP_PATH = new String[] {"properties", "@ns:com:here:xyz", "tags"};

  static final PRef ID = new PRef(ID_PROP_PATH);
  static final PRef APP_ID = new PRef(APP_ID_PROP_PATH);
  static final PRef AUTHOR = new PRef(AUTHOR_PROP_PATH);
  static final PRef UUID = new PRef(UUID_PROP_PATH);
  static final PRef MRID = new PRef(MRID_PROP_PATH);
  static final PRef GRID = new PRef(GRID_PROP_PATH);
  static final PRef TXN = new PRef(TXN_PROP_PATH);
  static final PRef TXN_NEXT = new PRef(TXN_NEXT_PROP_PATH);
  static final PRef TAGS = new PRef(TAGS_PROP_PATH);

  // Mapping of JSON Prop path to PRef object
  public static final Map<String[], PRef> PATH_TO_PREF_MAPPING = new HashMap<>() {};

  static {
    PATH_TO_PREF_MAPPING.put(ID_PROP_PATH, ID);
    PATH_TO_PREF_MAPPING.put(APP_ID_PROP_PATH, APP_ID);
    PATH_TO_PREF_MAPPING.put(AUTHOR_PROP_PATH, AUTHOR);
    PATH_TO_PREF_MAPPING.put(UUID_PROP_PATH, UUID);
    PATH_TO_PREF_MAPPING.put(MRID_PROP_PATH, MRID);
    PATH_TO_PREF_MAPPING.put(GRID_PROP_PATH, GRID);
    PATH_TO_PREF_MAPPING.put(TXN_PROP_PATH, TXN);
    PATH_TO_PREF_MAPPING.put(TXN_NEXT_PROP_PATH, TXN_NEXT);
    PATH_TO_PREF_MAPPING.put(TAGS_PROP_PATH, TAGS);
  }

  private @Nullable String tagName;

  private final @NotNull List<@NotNull String> propertyPath;

  /**
   * Returns the full-qualified path to the property.
   *
   * @return the full-qualified path to the property.
   */
  public @NotNull List<@NotNull String> getPath() {
    return propertyPath;
  }

  /**
   * If this is a tag-reference, returns the tag that is referred.
   *
   * @return the tag being referred, if this is a tag-reference.
   */
  public @Nullable String getTagName() {
    return tagName;
  }

  /**
   * Returns the reference to the {@code id} property.
   *
   * @return the reference to the {@code id} property.
   */
  public static @NotNull PRef id() {
    return ID;
  }

  /**
   * Returns the reference to the {@code app_id} property from the XYZ-Namespace.
   *
   * @return the reference to the {@code app_id} property from the XYZ-Namespace.
   */
  public static @NotNull PRef app_id() {
    return APP_ID;
  }

  /**
   * Returns the reference to the {@code author} property from the XYZ-Namespace.
   *
   * @return the reference to the {@code author} property from the XYZ-Namespace.
   */
  public static @NotNull PRef author() {
    return AUTHOR;
  }

  /**
   * Returns the reference to the virtual {@code mrid} XYZ property, which is a combination between {@code crid} and {@code grid}, with
   * {@code crid} overriding the {@code grid}.
   *
   * @return the reference to the virtual {@code mrid} property.
   */
  public static @NotNull PRef mrid() {
    return MRID;
  }

  /**
   * Returns the reference to the {@code grid} property from the XYZ-Namespace.
   *
   * @return the reference to the {@code grid} property from the XYZ-Namespace.
   */
  public static @NotNull PRef grid() {
    return GRID;
  }

  /**
   * Returns the reference to the {@code uuid} property from the XYZ-Namespace.
   *
   * @return the reference to the {@code uuid} property from the XYZ-Namespace.
   */
  public static @NotNull PRef uuid() {
    return UUID;
  }

  /**
   * Returns the reference to a specific tag from the {@code tags} array of the XYZ-Namespace.
   *
   * @return the reference to a specific tag from the {@code tags} array of the XYZ-Namespace.
   */
  public static @NotNull PRef tag(@NotNull CharSequence name) {
    final String tagName = string(name);
    assert tagName != null;
    return new PRef(TAGS_PROP_PATH).withTagName(tagName);
  }

  /**
   * Returns the reference to the {@code txn} property from the XYZ-Namespace.
   *
   * @return the reference to the {@code txn} property from the XYZ-Namespace.
   */
  public static @NotNull PRef txn() {
    return TXN;
  }

  /**
   * Returns the reference to the {@code txn_next} property from the XYZ-Namespace.
   *
   * @return the reference to the {@code txn_next} property from the XYZ-Namespace.
   */
  public static @NotNull PRef txn_next() {
    return TXN_NEXT;
  }
}
