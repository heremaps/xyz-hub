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
package com.here.naksha.app.service.http.auth;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.here.naksha.lib.core.models.auth.ActionMatrix;
import com.here.naksha.lib.core.models.auth.ServiceMatrix;
import io.vertx.core.json.jackson.DatabindCodec;
import org.jetbrains.annotations.Nullable;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_DEFAULT)
public class JWTPayload {

  /**
   * The authenticated application identifier; if any.
   */
  public String appId;
  /**
   * The authenticated user ID; if any.
   */
  public String userId;

  public int iat;
  public int exp;
  public ServiceMatrix urm;

  @JsonIgnore
  private XyzHubActionMatrix __nakshaMatrix; // TODO NakshaActionMatrix

  /**
   * Returns the Naksha action matrix, if there is any for this JWT token.
   *
   * @return the Naksha action matrix or null.
   */
  @JsonIgnore
  public @Nullable XyzHubActionMatrix getNakshaMatrix() {
    if (__nakshaMatrix != null) {
      return __nakshaMatrix;
    }
    if (urm == null) {
      return null;
    }
    final ActionMatrix hereActionMatrix = urm.get(URMServiceId.NAKSHA);
    if (hereActionMatrix == null) {
      return null;
    }
    return __nakshaMatrix = DatabindCodec.mapper().convertValue(hereActionMatrix, XyzHubActionMatrix.class);
  }

  /**
   * Constants for all services that may be part of the JWT token.
   */
  public static final class URMServiceId {
    static final String NAKSHA = "naksha";
  }
}
