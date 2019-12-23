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

import java.util.List;

@SuppressWarnings("serial")
public class XyzHubAttributeMap extends AttributeMap {

  public static final String OWNER = "owner";
  public static final String SPACE = "space";
  public static final String PACKAGES = "packages";
  public static final String STORAGE = "storage";
  public static final String LISTENERS = "listeners";
  public static final String PROCESSORS = "processors";
  public static final String SEARCHABLE_PROPERTIES = "searchableProperties";
  public static final String ID = "id";

  public static XyzHubAttributeMap forValues(String owner, String space, List<String> packages) {
    XyzHubAttributeMap attributeMap = new XyzHubAttributeMap();
    attributeMap.withValue(OWNER, owner).withValue(SPACE, space).withValue(PACKAGES, packages);
    return attributeMap;
  }

  public static XyzHubAttributeMap forIdValues(String id) {
    XyzHubAttributeMap attributeMap = new XyzHubAttributeMap();
    attributeMap.withValue(ID, id);
    return attributeMap;
  }

  public static XyzHubAttributeMap forIdValues(String owner, String id) {
    XyzHubAttributeMap attributeMap = new XyzHubAttributeMap();
    attributeMap.withValue(OWNER, owner);
    attributeMap.withValue(ID, id);
    return attributeMap;
  }

}
