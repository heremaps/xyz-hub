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
package com.here.naksha.common.http.apis;

public class ApiParamsConst {
  public static final String ACCESS_TOKEN = "access_token";
  public static final String STORAGE_ID = "storageId";
  public static final String SPACE_ID = "spaceId";
  public static final String PREFIX_ID = "prefixId";
  public static final String FEATURE_ID = "featureId";
  public static final String ADD_TAGS = "addTags";
  public static final String REMOVE_TAGS = "removeTags";
  public static final String TAGS = "tags";
  public static final String FEATURE_IDS = "id";
  public static final String WEST = "west";
  public static final String NORTH = "north";
  public static final String EAST = "east";
  public static final String SOUTH = "south";
  public static final String LIMIT = "limit";
  public static final String TILE_TYPE = "type";
  public static final String TILE_ID = "tileId";
  public static final String HANDLE = "handle";
  public static final String MARGIN = "margin";
  public static final String LAT = "lat";
  public static final String LON = "lon";
  public static final String RADIUS = "radius";
  public static final String REF_SPACE_ID = "refSpaceId";
  public static final String REF_FEATURE_ID = "refFeatureId";
  public static final String PROP_SELECTION = "selection";
  public static final String CLIP_GEO = "clip";
  public static final String PROPERTY_SEARCH_OP = "propertySearchOp";
  public static final long DEF_FEATURE_LIMIT = 30_000;
  public static final long DEF_ADMIN_FEATURE_LIMIT = 1_000;
  // Note - using specific NULL value is not ideal, but practically it makes code less messy at few places
  // and use of it doesn't cause any side effect
  public static final double NULL_COORDINATE = 9999;
  public static final String TILE_TYPE_QUADKEY = "quadkey";
  public static final String HANDLER_ID = "handlerId";
}
