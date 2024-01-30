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
package com.here.naksha.lib.core.models.naksha;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.NakshaVersion;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * A collection is a virtual container for features, managed by a {@link Storage}. All collections optionally have a history and transaction
 * log.
 */
@JsonTypeName(value = "NakshaCollection")
@AvailableSince(NakshaVersion.v2_0_7)
public class XyzCollection extends NakshaFeature {

  @AvailableSince(NakshaVersion.v2_0_7)
  public static final String ID_PREFIX = "idPrefix";

  @AvailableSince(NakshaVersion.v2_0_7)
  public static final String PARTITION = "partition";

  @AvailableSince(NakshaVersion.v2_0_7)
  public static final String POINTS_ONLY = "pointsOnly";

  @AvailableSince(NakshaVersion.v2_0_7)
  public static final String UNLOGGED = "unlogged";

  @AvailableSince(NakshaVersion.v2_0_7)
  public static final String DISABLE_HISTORY = "disableHistory";

  @AvailableSince(NakshaVersion.v2_0_11)
  public static final String AUTO_PURGE = "autoPurge";

  @AvailableSince(NakshaVersion.v2_0_7)
  public static final String MIN_AGE = "minAge";

  @AvailableSince(NakshaVersion.v2_0_7)
  public static final String ESTIMATED_FEATURE_COUNT = "estimatedFeatureCount";

  @AvailableSince(NakshaVersion.v2_0_7)
  public static final String ESTIMATED_DELETED_FEATURED = "estimatedDeletedFeatures";

  @AvailableSince(NakshaVersion.v2_0_7)
  public static final String PARTITION_COUNT = "partitionCount";

  /**
   * Create a new empty default collection with default properties.
   *
   * @param id The identifier of the collection.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonCreator
  public XyzCollection(@JsonProperty(ID) @NotNull String id) {
    this(id, false, false, false);
  }

  /**
   * Create a new collection with special properties.
   *
   * <p>This special purpose constructor allows to create partition with some special features. It is not guaranteed if the storage
   * implementation does support this.</p>
   *
   * @param id         The identifier of the collection.
   * @param partition  If the collection should be partitioned for better performance to improve bulk operations.
   * @param pointsOnly If only points will be stored in the collection, which allows to use different indexing, and to reduce the storage
   *                   space consumed.
   * @param unlogged   If the collection should be unlogged, this makes it not crash-safe, but much faster.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public XyzCollection(@NotNull String id, boolean partition, boolean pointsOnly, boolean unlogged) {
    super(id);
    this.partition = partition;
    this.pointsOnly = pointsOnly;
    this.partitionCount = -1;
    this.estimatedFeatureCount = -1L;
    this.estimatedDeletedFeatures = -1L;
  }

  /**
   * The maximum age of the history entries in days. Zero means the history should not be kept. no instant collection, but does not disable
   * the history.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonProperty(MIN_AGE)
  @JsonInclude(Include.ALWAYS)
  private long minAge = 3560L;

  /**
   * Toggle if the history is enabled.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonProperty(DISABLE_HISTORY)
  @JsonInclude(Include.NON_EMPTY)
  private boolean disableHistory;

  /**
   * Toggle if the auto-purge is enabled.
   */
  @AvailableSince(NakshaVersion.v2_0_11)
  @JsonProperty(AUTO_PURGE)
  @JsonInclude(Include.NON_EMPTY)
  private boolean autoPurge;

  /**
   * Returns {@code true} if this collection is partitioned.
   *
   * @return {@code true} if this collection is partitioned; {@code false} otherwise.
   */
  @JsonIgnore
  public boolean isPartitioned() {
    return partition;
  }

  /**
   * A value greater than zero implies that the collection shall be treated as deleted and represents the UTC Epoch timestamp in
   * milliseconds when the deletion has been done.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonProperty(PARTITION)
  @JsonInclude(Include.NON_EMPTY)
  private boolean partition;

  /**
   * Returns {@code true} if this collection is optimized for point geometry.
   *
   * @return {@code true} if this collection is optimized for point geometry; {@code false} otherwise, which means that it may not be
   * possible to store other geometry in it or even if done so, it may be suboptimal performance wise.
   */
  public boolean pointsOnly() {
    return pointsOnly;
  }

  /**
   * If the partition will store points only, this means the storage engine can use a different index type to reduce space consumption and
   * improve performance.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonProperty(POINTS_ONLY)
  @JsonInclude(Include.NON_EMPTY)
  private boolean pointsOnly;

  /**
   * Returns {@code true} if this collection is unlogged (optimized for performance, but not crash safe).
   *
   * @return {@code true} if this collection is unlogged (optimized for performance); {@code false} otherwise, which means that it may come
   * to data loss, if the database crashes while writing to it.
   */
  public boolean isUnlogged() {
    return unlogged;
  }

  /**
   * Changes the unlogged state.
   *
   * @param unlogged {@code true} if this collection should become unlogged (optimized for performance, but not crash safe).
   * @return The previous log-state.
   */
  public boolean setUnlogged(boolean unlogged) {
    final boolean old = this.unlogged;
    this.unlogged = unlogged;
    return old;
  }

  /**
   * If the partition should be unlogged, so not crash safe (improves the performance).
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonProperty(UNLOGGED)
  @JsonInclude(Include.NON_EMPTY)
  private boolean unlogged;

  /**
   * Returns the minimum age of the history in days.
   *
   * @return the minimum age of the history in days.
   */
  @JsonIgnore
  public long getMinAge() {
    return minAge;
  }

  /**
   * Sets the minimum age of the history in days. If the history is older, it can be garbage collected, but there is no exact time when the
   * storage will do this. Even setting the minimum age to zero, does not mean, that no history will exist.
   *
   * @param minAge the minimum age of the history in days.
   * @throws IllegalArgumentException If the given age is less than zero.
   */
  @JsonIgnore
  public void setMinAge(long minAge) {
    if (minAge < 0L) {
      throw new IllegalArgumentException("The minimum age must be at least zero");
    }
    this.minAge = minAge;
  }

  /**
   * Returns true if the history writer is currently enabled; false otherwise. Note that existing history is not impacted and unless
   * {@link #setMinAge(long)} is used to set minimum age. It is totally fine to disable the history write, perform some bulk operation and
   * then to re-enable the history. This may cause wholes in the history, but will keep existing history intact.
   *
   * @return true if the history writer is currently enabled; false otherwise.
   */
  @JsonIgnore
  public boolean hasHistory() {
    return !disableHistory;
  }

  /**
   * Enable or disable the history writer.
   *
   * @param history true to enable the history writer; false to disable it.
   */
  @JsonIgnore
  public void setHistory(boolean history) {
    disableHistory = !history;
  }

  /**
   * Enable the history writer.
   */
  @JsonIgnore
  public void enableHistory() {
    disableHistory = false;
  }

  /**
   * Disable the history writer.
   */
  @JsonIgnore
  public void disableHistory() {
    disableHistory = true;
  }

  /**
   * Returns true if the auto-purge is currently enabled; false otherwise.
   * Auto-purge decides whether features will be written to _del table on DELETE operation (auto-purge: false) or not (auto-purge: true).
   *
   * @return true if the auto-purge is currently enabled; false otherwise.
   */
  @JsonIgnore
  public boolean isAutoPurge() {
    return autoPurge;
  }

  /**
   * Enable or disable the auto-purge (inserts to _del).
   *
   * @param autoPurge true to enable the auto-purge; false to disable it.
   */
  @JsonIgnore
  public void setAutoPurge(boolean autoPurge) {
    this.autoPurge = autoPurge;
  }

  /**
   * Enable auto-purge.
   */
  @JsonIgnore
  public void enableAutoPurge() {
    autoPurge = true;
  }

  /**
   * Disable auto-purge.
   */
  @JsonIgnore
  public void disableAutoPurge() {
    autoPurge = false;
  }

  /**
   * Returns the amount of partitions, which is a necessary information for the bulk loader. This returns zero, if the collection is not
   * partitioned. This information can only be obtained from the storage itself when reading a collection or as result of creating a
   * collection.
   *
   * @return the amount of partitions, which is a necessary information for the bulk loader. The storage returns minus one, if the
   * information is not known.
   */
  public int partitionCount() {
    return partitionCount;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonProperty(PARTITION_COUNT)
  @JsonInclude(Include.NON_EMPTY)
  private int partitionCount;

  /**
   * Returns the amount of features being alive in the collection. This returns minus one, if the amount can not be provided by the storage.
   * This information can only be obtained from the storage itself when reading a collection or as result of creating a collection.
   *
   * @return the estimated amount of alive feature or minus one, if the storage can't provide this information.
   */
  public long estimatedFeatureCount() {
    return estimatedFeatureCount;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonProperty(ESTIMATED_FEATURE_COUNT)
  @JsonInclude(Include.NON_EMPTY)
  private long estimatedFeatureCount;

  /**
   * Returns the amount of dead features (being in deleted state), but not yet purged in the collection. This returns minus one, if the
   * amount can not be provided by the storage. This information can only be obtained from the storage itself when reading a collection or
   * as result of creating a collection.
   *
   * @return the estimated amount of dead feature (being in deleted state) or minus one, if the storage can't provide this information.
   */
  public long estimatedDeletedFeatures() {
    return estimatedDeletedFeatures;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonProperty(ESTIMATED_DELETED_FEATURED)
  @JsonInclude(Include.NON_EMPTY)
  private long estimatedDeletedFeatures;
}
