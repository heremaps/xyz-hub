/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

import java.util.List;
import java.util.Map;

public class GetStorageStatisticsEvent extends Event<GetStorageStatisticsEvent> {

  private List<String> spaceIds;
  private Map<String, String> spaceIdToTableName;

  public List<String> getSpaceIds() {
    return spaceIds;
  }

  public void setSpaceIds(List<String> spaceIds) {
    this.spaceIds = spaceIds;
  }

  public GetStorageStatisticsEvent withSpaceIds(List<String> spaceIds) {
    setSpaceIds(spaceIds);
    return this;
  }

  /**
   * The physical table names of the requested spaces, keyed by their space ID.
   * @return The physical table names, keyed by space ID
   */
  public Map<String, String> getSpaceIdToTableName() {
    return spaceIdToTableName;
  }

  public void setSpaceIdToTableName(Map<String, String> spaceIdToTableName) {
    this.spaceIdToTableName = spaceIdToTableName;
  }

  public GetStorageStatisticsEvent withSpaceIdToTableName(Map<String, String> spaceIdToTableName) {
    setSpaceIdToTableName(spaceIdToTableName);
    return this;
  }
}
