/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.xyz.hub.rest;

/**
 * An enumeration with all responses that should be returned to the client. If the required response type is not available and {@link
 * com.here.xyz.responses.ErrorResponse} should be returned.
 */
public enum ApiResponseType {
  EMPTY,
  FEATURE,
  FEATURE_COLLECTION,
  MVT,
  MVT_FLATTENED,
  SPACE,
  SPACE_LIST,
  @Deprecated
  COUNT_RESPONSE,
  HEALTHY_RESPONSE,
  STATISTICS_RESPONSE
}
