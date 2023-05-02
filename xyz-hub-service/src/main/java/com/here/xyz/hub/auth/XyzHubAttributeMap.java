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

import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.XyzError;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class XyzHubAttributeMap extends AttributeMap {

  @Deprecated
  public static final String ID = "id";
  public static final String OWNER = "owner";
  public static final String SPACE = "space";
  public static final String CONNECTOR = "connector";
  public static final String PACKAGES = "packages";
  @Deprecated
  public static final String STORAGE = "storage";
  @Deprecated
  public static final String LISTENERS = "listeners";
  @Deprecated
  public static final String PROCESSORS = "processors";
  @Deprecated
  public static final String SEARCHABLE_PROPERTIES = "searchableProperties";
  @Deprecated
  public static final String SORTABLE_PROPERTIES = "sortableProperties";

  public static @NotNull AttributeMap forSpace(@NotNull Space space) {
    final AttributeMap attributeMap = new AttributeMap();
    attributeMap.withValue(XyzHubAttributeMap.SPACE, space.getId());
    attributeMap.withValue(XyzHubAttributeMap.OWNER, space.properties().useXyzNamespace().getOwner());
    if (space.getPackages() != null) {
      attributeMap.withValue(XyzHubAttributeMap.PACKAGES, space.getPackages()); // oneOf
    }
    return attributeMap;
  }

  public static @NotNull AttributeMap forPackage(@NotNull String packageId) {
    final AttributeMap attributeMap = new AttributeMap();
    attributeMap.withValue(XyzHubAttributeMap.PACKAGES, packageId);
    return attributeMap;
  }

  public static @NotNull AttributeMap forConnector(@NotNull Connector connector) {
    final AttributeMap attributeMap = new AttributeMap();
    attributeMap.withValue(XyzHubAttributeMap.CONNECTOR, connector.id);
    attributeMap.withValue(XyzHubAttributeMap.OWNER, connector.properties().useXyzNamespace().getOwner());
    if (connector.packages != null) {
      attributeMap.withValue(XyzHubAttributeMap.PACKAGES, connector.packages); // oneOf
    }
    return attributeMap;
  }

  public static @NotNull AttributeMap forConnectorId(@NotNull String connectorId) throws XyzErrorException {
    final Connector connector = Connector.getConnectorById(connectorId);
    if (connector == null) {
      throw new XyzErrorException(XyzError.FORBIDDEN, "Unknown connector " + connectorId);
    }
    return forConnector(connector);
  }

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
