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

package com.here.xyz.hub.auth;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import io.vertx.core.json.Json;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_DEFAULT)
public class JWTPayload {

  public String tid;
  public String aid;
  public String cid;

  public Map<String, Object> metadata;

  public int exp;
  public ServiceMatrix urm;
  public XYZUsageLimits limits;
  public boolean anonymous;

  /**
   * Returns the XYZ Hub action matrix, if there is any for this JWT token.
   * @return the XYZ Hub action matrix or null.
   */
  @JsonIgnore
  public XyzHubActionMatrix getXyzHubMatrix(){
    if (urm == null)
      return null;
    final ActionMatrix hereActionMatrix = urm.get(URMServiceId.XYZ_HUB);
    if (hereActionMatrix == null)
      return null;
    return Json.mapper.convertValue(hereActionMatrix, XyzHubActionMatrix.class);
  }

  /**
   * Constants for all services that may be part of the JWT token.
   */
  public static final class URMServiceId {
    static final String XYZ_HUB = "xyz-hub";
  }
}
