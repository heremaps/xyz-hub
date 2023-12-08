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
package com.here.naksha.lib.core.models.storage;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.naksha.lib.core.models.geojson.coordinates.JTSHelper;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzGeometry;
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.util.json.Json;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The default codec for XYZ core library, this can simply be specialized.
 */
public class XyzCodec<FEATURE extends XyzFeature, SELF extends XyzCodec<FEATURE, SELF>>
    extends FeatureCodec<FEATURE, SELF> {

  XyzCodec(@NotNull Class<FEATURE> featureClass) {
    this.featureClass = featureClass;
  }

  private final @NotNull Class<FEATURE> featureClass;

  @NotNull
  @Override
  public final SELF decodeParts(boolean force) {
    if (!force && isDecoded) {
      return self();
    }
    if (feature == null) {
      throw new NullPointerException();
    }
    XyzGeometry xyzGeometry = feature.removeGeometry();
    try (final Json jp = Json.get()) {
      id = feature.getId();
      final XyzProperties properties = feature.getProperties();
      final XyzNamespace xyz = properties.getXyzNamespace();
      uuid = xyz.getUuid();
      if (feature.get("type") instanceof String) {
        featureType = (String) feature.get("type");
      } else if (feature.get("momType") instanceof String) {
        featureType = (String) feature.get("momType");
      } else {
        // TODO: Let's tell Jackson, that we want to keep the "type" property to prevent this!
        featureType = null;
      }
      if (feature.getProperties().get("type") instanceof String) {
        propertiesType = (String) feature.getProperties().get("type");
      } else if (feature.getProperties().get("featureType") instanceof String) {
        propertiesType = (String) feature.getProperties().get("featureType");
      } else if (feature.get("momType") instanceof String) {
        propertiesType = (String) feature.get("momType");
      } else {
        propertiesType = null;
      }
      if (xyzGeometry != null) {
        geometry = xyzGeometry.getJTSGeometry();
      } else {
        geometry = null;
      }
      wkb = null;
      json = jp.writer().writeValueAsString(feature);
    } catch (JsonProcessingException e) {
      throw unchecked(e);
    } finally {
      feature.setGeometry(xyzGeometry);
      isDecoded = true;
    }
    return self();
  }

  @Nullable
  @Override
  public final SELF encodeFeature(boolean force) {
    if (!force && isEncoded) {
      return self();
    }
    if (json == null) {
      return self();
    }
    feature = null;
    try (final Json jp = Json.get()) {
      feature = jp.reader().forType(featureClass).readValue(json);
      feature.setGeometry(JTSHelper.fromGeometry(getGeometry()));
    } catch (JsonProcessingException e) {
      throw unchecked(e);
    } finally {
      isEncoded = true;
    }
    return self();
  }
}
