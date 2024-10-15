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

package com.here.xyz.events;

public record UpdateStrategy(OnExists onExists, OnNotExists onNotExists, OnVersionConflict onVersionConflict,
    OnMergeConflict onMergeConflict) {
  public static final UpdateStrategy DEFAULT_UPDATE_STRATEGY = new UpdateStrategy(OnExists.REPLACE, OnNotExists.CREATE, null,
      null);
  public static final UpdateStrategy DEFAULT_DELETE_STRATEGY = new UpdateStrategy(OnExists.DELETE, OnNotExists.RETAIN, null,
      null);

  public enum OnExists {
    DELETE,
    REPLACE,
    RETAIN,
    ERROR
  }

  public enum OnNotExists {
    CREATE,
    RETAIN,
    ERROR
  }

  public enum OnVersionConflict {
    MERGE,
    REPLACE,
    RETAIN,
    ERROR,
    DELETE
  }

  public enum OnMergeConflict {
    REPLACE,
    RETAIN,
    ERROR //Default
  }
}
