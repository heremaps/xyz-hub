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
package com.here.naksha.lib.core.models.geojson.implementation.namespaces;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.util.json.JsonObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings({"UnusedReturnValue", "unused"})
public class HereDeltaNs extends JsonObject {

  /**
   * Create a new default delta namespace for new features.
   */
  public HereDeltaNs() {
    this.changeState = EChangeState.CREATED;
    this.reviewState = EReviewState.UNPUBLISHED;
  }

  /**
   * When features are deserialized.
   *
   * @param changeState The change-state.
   * @param reviewState The review-state.
   */
  @JsonCreator
  public HereDeltaNs(
      @JsonProperty("changeState") @Nullable EChangeState changeState,
      @JsonProperty("reviewState") @Nullable EReviewState reviewState) {
    if (changeState == null) {
      changeState = EChangeState.CREATED;
    }
    if (reviewState == null) {
      reviewState = EReviewState.UNPUBLISHED;
    }
    this.changeState = changeState;
    this.reviewState = reviewState;
  }

  /**
   * The stream-id that produced this state.
   */
  @Deprecated
  @JsonProperty
  private @Nullable String streamId;

  @Deprecated
  @JsonProperty
  private long changeCounter;

  /**
   * The origin-ID is a write-once value and must refer to an existing base object. Therefore, there is no guarantee that when this property
   * is set, the corresponding base object does exist, it is only guaranteed that this object existed at the time the origin-ID was set. If
   * a base object is modified the originId is always set automatically and refers to itself.
   */
  @JsonProperty
  private String originId;

  /**
   * Whenever a new object is created, the change-counter is reset to 1. When an existing object is modified the change counter is
   * incremented for every change.
   */
  public @Nullable String getOriginId() {
    return originId;
  }

  /**
   * Whenever a new object is created, the change-counter is reset to 1. When an existing object is modified the change counter is
   * incremented for every change.
   */
  public @Nullable String setOriginId(@Nullable String originId) {
    final String old = this.originId;
    this.originId = originId;
    return old;
  }

  /**
   * Whenever a new object is created, the change-counter is reset to 1. When an existing object is modified the change counter is
   * incremented for every change.
   */
  public @NotNull HereDeltaNs withOriginId(@Nullable String originId) {
    setOriginId(originId);
    return this;
  }

  /**
   * When a feature is split, all children must have the <b>parentLink</b> property referring to the feature that has the <b>changeState</b>
   * set to <b>SPLIT</b>. If an object has a <b>parentLink</b> property and the object referred does have an <b>originId</b> set, then this
   * children will automatically derive the <b>originId</b> value. This tracks all changes done back to the original object being modified.
   */
  @JsonProperty
  private String parentLink;

  /**
   * When a feature is split, all children must have the <b>parentLink</b> property referring to the feature that has the <b>changeState</b>
   * set to <b>SPLIT</b>. If an object has a <b>parentLink</b> property and the object referred does have an <b>originId</b> set, then this
   * children will automatically derive the <b>originId</b> value. This tracks all changes done back to the original object being modified.
   */
  public @Nullable String getParentLink() {
    return parentLink;
  }

  /**
   * When a feature is split, all children must have the <b>parentLink</b> property referring to the feature that has the <b>changeState</b>
   * set to <b>SPLIT</b>. If an object has a <b>parentLink</b> property and the object referred does have an <b>originId</b> set, then this
   * children will automatically derive the <b>originId</b> value. This tracks all changes done back to the original object being modified.
   */
  public @Nullable String setParentLink(@Nullable String parentLink) {
    final String old = this.parentLink;
    this.parentLink = parentLink;
    return old;
  }

  /**
   * When a feature is split, all children must have the <b>parentLink</b> property referring to the feature that has the <b>changeState</b>
   * set to <b>SPLIT</b>. If an object has a <b>parentLink</b> property and the object referred does have an <b>originId</b> set, then this
   * children will automatically derive the <b>originId</b> value. This tracks all changes done back to the original object being modified.
   */
  public @NotNull HereDeltaNs withParentLink(@Nullable String parentLink) {
    setParentLink(parentLink);
    return this;
  }

  /**
   * The change-state of the feature.
   */
  @JsonProperty
  private @NotNull EChangeState changeState;

  /**
   * The change-state of the feature.
   */
  public @NotNull EChangeState getChangeState() {
    return changeState;
  }

  /**
   * The change-state of the feature.
   */
  public @NotNull EChangeState setChangeState(@NotNull EChangeState changeState) {
    final EChangeState old = this.changeState;
    this.changeState = changeState;
    return old;
  }

  /**
   * The change-state of the feature.
   */
  public @NotNull HereDeltaNs withChangeState(@NotNull EChangeState changeState) {
    setChangeState(changeState);
    return this;
  }

  /**
   * The review-state of the feature.
   */
  @JsonProperty
  private @NotNull EReviewState reviewState;

  /**
   * The review-state of the feature.
   */
  public @NotNull EReviewState getReviewState() {
    return reviewState;
  }

  /**
   * The review-state of the feature.
   */
  public @NotNull EReviewState setReviewState(@NotNull EReviewState reviewState) {
    final EReviewState old = this.reviewState;
    this.reviewState = reviewState;
    return old;
  }

  /**
   * The review-state of the feature.
   */
  public @NotNull HereDeltaNs withReviewState(@NotNull EReviewState reviewState) {
    setReviewState(reviewState);
    return this;
  }

  /**
   * This value is currently set to 0 as default values. Any client operating in <b>MODERATION</b> or <b>BOT</b> mode can set this value to
   * whatever value he wants; normal users may not explicitly set this property, therefore for them the value is kept as it is, when not
   * existing, it is set 0 for normal users.
   */
  @JsonProperty
  private long potentialValue;

  /**
   * The priority category assigned to this edit. A value between 0 (no priority, no SLA) and 9. It is never valid to decrease the value
   * (see <a href="https://devzone.it.here.com/jira/browse/CMECMSSUP-1945">CMECMSSUP-1945</a>)!
   */
  @JsonProperty
  private long priorityCategory;

  /**
   * The UNIX epoch timestamp in milliseconds of the time until when the edit must be taken care of. This property is only set automatically
   * for edits. If the SLA is {@code null} or the <b>priorityCategory</b> is 0 (so, no priority), then the value will be set to 0; otherwise
   * the `meta::lastUpdatedTS` time plus the `SLA::dueIn` value will be taken to calculate this value (except the `SLA::dueIn` is 0, then
   * `meta::dueTS` is as well 0). It is never valid to increase the value (see <a
   * href="https://devzone.it.here.com/jira/browse/CMECMSSUP-1945">CMECMSSUP-1945</a>). We treat 0 as the highest value, therefore it can
   * not override any other **dueTS** value. Be aware that the `SLA::dueIn` is set in seconds, while the **dueTS** property is set in
   * milliseconds.
   */
  @JsonProperty
  private String dueTS;
}
