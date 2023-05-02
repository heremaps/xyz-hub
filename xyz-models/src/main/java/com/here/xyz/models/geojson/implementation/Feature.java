/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz.models.geojson.implementation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.Typed;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;

/**
 * A standard GeoJson feature.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "Feature")
@SuppressWarnings({"unused", "WeakerAccess"})
public class Feature extends AbstractFeature<Properties, Feature> implements Typed {

  /**
   * Create a new empty feature.
   */
  public Feature() {
    super();
  }

  @Override
  protected @NotNull Properties newProperties() {
    return new Properties();
  }

  /**
   * Creates a new Feature object, for which the properties and the xyz namespace are initialized.
   *
   * @return the feature with initialized properties and xyz namespace.
   */
  public static Feature createEmptyFeature() {
    return new Feature().withProperties(new Properties().withXyzNamespace(new XyzNamespace()));
  }

  /**
   * Updates the {@link XyzNamespace} properties in a feature so that all values are valid.
   *
   * @param feature the feature for which to update the {@link XyzNamespace} map.
   * @param space the full qualified space identifier in which this feature is stored (so prefix and base part combined, e.g. "x-foo").
   * @param addUUID If the uuid to be added or not.
   * @throws NullPointerException if feature, space or the 'id' of the feature are null.
   */
  @SuppressWarnings("WeakerAccess")
  public static void finalizeFeature(final Feature feature, final String space, boolean addUUID) throws NullPointerException {
    final XyzNamespace xyzNamespace = feature.getProperties().getXyzNamespace();

    if (addUUID) {
      String puuid = xyzNamespace.getUuid();
      if (puuid != null) {
        xyzNamespace.setPuuid(puuid);
      }
      xyzNamespace.setUuid(UUID.randomUUID().toString());
    }
  }
}
