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
package com.here.naksha.lib.core.storage;

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The storage result-set.
 *
 * @param <FEATURE> the feature-type.
 */
public interface IResultSet<FEATURE extends XyzFeature> extends AutoCloseable {

  /**
   * Load the next feature.
   *
   * @return {@code true} if another feature loaded; {@code false} otherwise.
   */
  boolean next();

  /**
   * Returns the identifier of the feature.
   *
   * @return the identifier of the feature.
   * @throws NoSuchElementException if no feature loaded.
   * @throws Exception              if any error occurred while parsing the feature or geometry.
   */
  @NotNull
  String getId() throws Exception;

  /**
   * Returns the state UUID of the feature.
   *
   * @return the state UUID of the feature.
   * @throws NoSuchElementException if no feature loaded.
   * @throws Exception              if any error occurred while parsing the feature or geometry.
   */
  @NotNull
  String getUuid() throws Exception;

  /**
   * Returns the JSON of the feature without the geometry, including the ID and UUID.
   *
   * @return the JSON of the feature without the geometry.
   * @throws NoSuchElementException if no feature loaded.
   * @throws Exception              if any error occurred while parsing the feature or geometry.
   */
  @NotNull
  String getJson() throws Exception;

  /**
   * Returns the hex-encoded WKB (well-known binary) of the feature or {@code null}, if the feature has no geometry.
   *
   * @return the hex-encoded WKB of the geometry or {@code null}, if the feature does not have a geometry.
   * @throws NoSuchElementException if no feature loaded.
   * @throws Exception              if any error occurred while parsing the feature or geometry.
   */
  @Nullable
  String getGeometry() throws Exception;

  /**
   * Decodes the feature and returns it.
   *
   * @return the decoded feature.
   * @throws NoSuchElementException if no feature loaded.
   * @throws Exception              if any error occurred while parsing the feature or geometry.
   */
  @NotNull
  FEATURE getFeature() throws Exception;

  /**
   * Decode features into a list.
   *
   * @param skip  the amount of features to skip.
   * @param limit the maximal amount of features to return.
   * @return the list with the read features, less or equal to {@code limit}.
   * @throws NoSuchElementException if no feature loaded.
   * @throws Exception              if any error occurred while parsing the feature or geometry.
   */
  default @NotNull List<@NotNull FEATURE> toList(int skip, int limit) throws Exception {
    final ArrayList<FEATURE> list = new ArrayList<>();
    int i = 0;
    while (i < skip && next()) {
      i++;
    }
    i = 0;
    while (i < limit && next()) {
      list.add(getFeature());
      i++;
    }
    return list;
  }

  @Override
  void close();
}
