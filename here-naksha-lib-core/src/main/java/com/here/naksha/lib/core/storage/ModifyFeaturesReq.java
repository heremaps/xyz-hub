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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Creates a modify features request.
 *
 * @param <FEATURE> the feature type store.
 */
@Deprecated
public class ModifyFeaturesReq<FEATURE extends XyzFeature> {

  private final List<FEATURE> insert;
  private final List<FEATURE> update;
  private final List<FEATURE> upsert;
  private final boolean readResults;
  private final List<DeleteOp> delete;

  /**
   * @param insert       the feature to insert.
   * @param update       the features to update.
   * @param upsert       the features to insert or update.
   * @param delete       the features to delete.
   * @param read_results {@code true} to request results from the database; {@code false} if the database should not send back results.
   */
  public ModifyFeaturesReq(
      @NotNull List<@NotNull FEATURE> insert,
      @NotNull List<@NotNull FEATURE> update,
      @NotNull List<@NotNull FEATURE> upsert,
      @NotNull List<@NotNull DeleteOp> delete,
      boolean read_results) {
    this.insert = insert;
    this.update = update;
    this.delete = delete;
    this.upsert = upsert;
    readResults = read_results;
  }

  /**
   * Create an empty modify features request reading the results.
   */
  public ModifyFeaturesReq() {
    this(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), true);
  }

  /**
   * Create a modify features request reading or not reading results.
   *
   * @param read_results {@code true} to request results from the database; {@code false} if the database should not send back results.
   */
  public ModifyFeaturesReq(boolean read_results) {
    this(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), read_results);
  }

  public List<FEATURE> getInsert() {
    return insert;
  }

  public List<FEATURE> getUpdate() {
    return update;
  }

  public List<FEATURE> getUpsert() {
    return upsert;
  }

  public boolean isReadResults() {
    return readResults;
  }

  public List<DeleteOp> getDelete() {
    return delete;
  }

  /**
   * Returns the total size, amount of features to insert, update, upsert and/or delete.
   *
   * @return the total size.
   */
  public int totalSize() {
    return insert.size() + update.size() + upsert.size() + delete.size();
  }

  /**
   * Add the given feature to the insertion list.
   *
   * @param feature The feature to add.
   * @return this.
   */
  public @NotNull ModifyFeaturesReq<FEATURE> insert(@NotNull FEATURE feature) {
    insert.add(feature);
    return this;
  }

  /**
   * Add the given feature to the update list.
   *
   * @param feature The feature to update.
   * @return this.
   */
  public @NotNull ModifyFeaturesReq<FEATURE> update(@NotNull FEATURE feature) {
    update.add(feature);
    return this;
  }

  /**
   * Add the given feature to the upsert list.
   *
   * @param feature The feature to upsert.
   * @return this.
   */
  public @NotNull ModifyFeaturesReq<FEATURE> upsert(@NotNull FEATURE feature) {
    // TODO HP_QUERY : Should this be added to upsert list?
    update.add(feature);
    return this;
  }

  /**
   * Add the given feature to the delete list.
   *
   * @param id The ID of the feature to delete.
   * @return this.
   */
  public @NotNull ModifyFeaturesReq<FEATURE> delete(@NotNull String id) {
    delete.add(new DeleteOp(id));
    return this;
  }

  /**
   * Add the given feature to the delete list.
   *
   * @param id   The ID of the feature to delete.
   * @param uuid The UUID of the state to delete.
   * @return this.
   */
  public @NotNull ModifyFeaturesReq<FEATURE> delete(@NotNull String id, @Nullable String uuid) {
    delete.add(new DeleteOp(id, uuid));
    return this;
  }
}
