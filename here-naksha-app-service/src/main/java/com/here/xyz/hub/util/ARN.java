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
package com.here.xyz.hub.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;

public class ARN {

  private String[] nonResourceParts;
  private String resource;

  public ARN(String arn) {
    String[] arnParts = arn.split(":");
    nonResourceParts = Arrays.copyOfRange(arnParts, 0, 5);
    int resourcePartIndex = String.join(":", nonResourceParts).length() + 1;
    resource = arn.substring(resourcePartIndex);
  }

  public String getPartition() {
    return nonResourceParts[1];
  }

  public String getService() {
    return nonResourceParts[2];
  }

  public String getRegion() {
    return nonResourceParts[3];
  }

  public String getAccountId() {
    return nonResourceParts[4];
  }

  public String getResource() {
    return resource;
  }

  public String getResourceWithoutType() {
    if (!resource.contains(":") && !resource.contains("/")) return getResource();

    int typeSeparatorPos = resource.indexOf(":");
    typeSeparatorPos = typeSeparatorPos != -1 ? typeSeparatorPos : resource.indexOf("/");

    return resource.substring(typeSeparatorPos + 1);
  }

  @JsonCreator
  public static ARN fromString(String arn) {
    if (StringUtils.isEmpty(arn) || !arn.contains(":")) return null;
    return new ARN(arn);
  }

  @JsonValue
  @Override
  public String toString() {
    return String.join(":", nonResourceParts) + ":" + resource;
  }
}
