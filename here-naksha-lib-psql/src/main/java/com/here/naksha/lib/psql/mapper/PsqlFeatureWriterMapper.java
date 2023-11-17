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
package com.here.naksha.lib.psql.mapper;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzGeometry;
import com.here.naksha.lib.core.util.json.Json;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBWriter;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class PsqlFeatureWriterMapper {

  private final WKBWriter wkbWriter = new WKBWriter(3);

  public <FEATURE extends XyzFeature> FeatureWriteInputParameters prepareFeatureWriteInputParameters(
      List<WriteOp<FEATURE>> writeOpsReq) {

    ArrayList<byte[]> geometries = new ArrayList<>(writeOpsReq.size());
    ArrayList<String> operationsJson = new ArrayList<>(writeOpsReq.size());

    for (WriteOp<FEATURE> writeOp : writeOpsReq) {
      FEATURE feature = writeOp.feature;
      final XyzGeometry nkGeometry = feature.getGeometry();
      feature.setGeometry(null);
      try (final Json json = Json.get()) {
        geometries.add(serializeGeometry(feature.getGeometry()));

        final String serializedWriteOp = json.writer().writeValueAsString(writeOp);
        operationsJson.add(serializedWriteOp);
      } catch (final Throwable t) {
        throw unchecked(t);
      } finally {
        feature.setGeometry(nkGeometry);
      }
    }

    return new FeatureWriteInputParameters(geometries, operationsJson);
  }

  private byte[] serializeGeometry(XyzGeometry nkGeometry) {
    byte[] serialized = null;
    if (nkGeometry != null) {
      Geometry jtsGeometry = nkGeometry.getJTSGeometry();
      assure3d(jtsGeometry.getCoordinates());
      serialized = wkbWriter.write(jtsGeometry);
    }
    return serialized;
  }

  private static void assure3d(@NotNull Coordinate @NotNull [] coords) {
    for (final @NotNull Coordinate coord : coords) {
      if (coord.z != coord.z) { // if coord.z is NaN
        coord.z = 0;
      }
    }
  }
}
