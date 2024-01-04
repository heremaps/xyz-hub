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
package com.here.naksha.lib.core.models.tx;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.here.naksha.lib.core.NakshaVersion;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.Nullable;

// TODO: This class is not yet final!
public enum UniMapTxAction {
  MODIFY_FEATURES,
  CREATE_COLLECTION,
  UPDATE_COLLECTION,
  DELETE_COLLECTION,
  DROP_COLLECTION;

  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonCreator
  public static @Nullable UniMapTxAction of(@Nullable String value) {
    if (value != null) {
      for (final UniMapTxAction e : UniMapTxAction.values()) {
        if (e.name().equalsIgnoreCase(value)) {
          return e;
        }
      }
    }
    return null;
  }
}
