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
package com.here.naksha.lib.core.models.geojson.implementation.namespaces;

import com.here.naksha.lib.core.util.json.JsonEnum;
import org.jetbrains.annotations.NotNull;

/**
 * The review state.
 */
@SuppressWarnings("unused")
public class EReviewState extends JsonEnum {

  /**
   * This is the initial state for any un-moderated feature. The default for all new features.
   */
  public static final EReviewState UNPUBLISHED = defIgnoreCase(EReviewState.class, "UNPUBLISHED");

  /**
   * Set by the auto-endorser, if the feature is ready to be sent into the bucket-processor.
   */
  public static final EReviewState AUTO_ENDORSED = defIgnoreCase(EReviewState.class, "AUTO_ENDORSED");

  /**
   * Set by the auto-endorser, if the change should be reverted.
   */
  public static final EReviewState AUTO_ROLLBACK =
      defIgnoreCase(EReviewState.class, "AUTO_ROLLBACK").with(EReviewState.class, EReviewState::setFinalState);

  /**
   * Set by the auto-endorser, if the feature must be reviewed by a moderator.
   */
  public static final EReviewState AUTO_REVIEW_DEFERRED = defIgnoreCase(EReviewState.class, "AUTO_REVIEW_DEFERRED");

  /**
   * Set by the change-set-publisher, if the feature was integrated into consistent-store.
   */
  public static final EReviewState AUTO_INTEGRATED =
      defIgnoreCase(EReviewState.class, "AUTO_INTEGRATED").with(EReviewState.class, EReviewState::setFinalState);

  /**
   * Set by the change-set-publisher, if the feature integration failed and more moderation is needed.
   */
  public static final EReviewState FAILED = defIgnoreCase(EReviewState.class, "FAILED");

  /**
   * Set by a moderator, when the feature is ready to be send to the bucket-processor.
   */
  public static final EReviewState ENDORSED = defIgnoreCase(EReviewState.class, "ENDORSED");

  /**
   * Set by a moderator, when the feature need more moderation.
   */
  public static final EReviewState UNDECIDED = defIgnoreCase(EReviewState.class, "UNDECIDED");

  /**
   * Set by a moderator, when the feature is rejected, the change should be reverted.
   */
  public static final EReviewState ROLLBACK =
      defIgnoreCase(EReviewState.class, "ROLLBACK").with(EReviewState.class, EReviewState::setFinalState);

  /**
   * Set by a moderator, when the feature was manually coded into RMOB. In-between state, that eventually will be changed into
   * {@link #AUTO_INTEGRATED}.
   */
  public static final EReviewState INTEGRATED = defIgnoreCase(EReviewState.class, "INTEGRATED");

  /**
   * If this is a final state.
   */
  private boolean isFinalState;

  private static void setFinalState(@NotNull EReviewState self) {
    self.isFinalState = true;
  }

  /**
   * Returns if this is a final state.
   * @return if this is a final state.
   */
  public boolean isFinalState() {
    return isFinalState;
  }

  @Override
  protected void init() {
    register(EReviewState.class);
  }
}
