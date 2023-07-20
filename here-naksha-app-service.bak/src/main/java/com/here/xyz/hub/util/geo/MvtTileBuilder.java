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
package com.here.xyz.hub.util.geo;

import static com.here.xyz.hub.util.geo.GeoTools.WEB_MERCATOR_EPSG;
import static com.here.xyz.hub.util.geo.GeoTools.WGS84_EPSG;

import com.here.naksha.lib.core.models.geojson.WebMercatorTile;
import com.here.naksha.lib.core.models.geojson.implementation.Feature;
import com.here.naksha.lib.core.models.geojson.implementation.Geometry;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.VectorTile.Tile;
import com.wdtinc.mapbox_vector_tile.adapt.jts.IGeometryFilter;
import com.wdtinc.mapbox_vector_tile.adapt.jts.IUserDataConverter;
import com.wdtinc.mapbox_vector_tile.adapt.jts.JtsAdapter;
import com.wdtinc.mapbox_vector_tile.adapt.jts.TileGeomResult;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerBuild;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerParams;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerProps;
import java.util.List;
import org.geotools.geometry.jts.JTS;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

/**
 * A helper class to build a pixel based MapBox Vector Tiles.
 */
public abstract class MvtTileBuilder implements IUserDataConverter, IGeometryFilter {

  private Feature feature;
  protected MvtLayerProps layerProps;
  protected VectorTile.Tile.Feature.Builder featureBuilder;

  /**
   * Create a new empty tile creator.
   */
  public MvtTileBuilder() {}

  /**
   * Create a new tile with only one layer that contains the given features.
   */
  public byte[] build(WebMercatorTile wmTile, int margin, String layerName, List<Feature> featureList)
      throws Exception {

    final MathTransform mathTransform = GeoTools.mathTransform(WGS84_EPSG, WEB_MERCATOR_EPSG);
    final GeometryFactory geomFactory = new GeometryFactory(new PrecisionModel());

    Envelope tileEnvelope = new Envelope(wmTile.left, wmTile.right, wmTile.bottom, wmTile.top);
    Envelope clipEnvelope = new Envelope(tileEnvelope);
    clipEnvelope.expandBy(margin * (Math.abs(wmTile.left - wmTile.right) / WebMercatorTile.TileSizeInPixel));

    // Prepare a layer (we will for now only have one layer per tile).
    final MvtLayerParams layerParams = new MvtLayerParams();
    final VectorTile.Tile.Layer.Builder layerBuilder = MvtLayerBuild.newLayerBuilder(layerName, layerParams);
    final MvtLayerProps layerProperties = new MvtLayerProps();
    final VectorTile.Tile.Builder tileBuilder = VectorTile.Tile.newBuilder();

    // Add all features with their geometry and properties.
    if (featureList != null) {
      for (int f = 0; f < featureList.size(); f++) {
        final Feature feature = featureList.get(f);
        if (feature == null) {
          continue;
        }

        final Geometry featureGeometry = feature.getGeometry();
        if (featureGeometry == null) {
          continue;
        }
        final com.vividsolutions.jts.geom.Geometry wgs84Geometry = featureGeometry.getJTSGeometry();
        if (wgs84Geometry == null) {
          continue;
        }

        com.vividsolutions.jts.geom.Geometry targetGeometry;
        try {
          targetGeometry = JTS.transform(wgs84Geometry, mathTransform);
        } catch (MismatchedDimensionException | TransformException e) {
          onTransformException(e);
          continue;
        }

        try {
          targetGeometry = GeoTools.validate(targetGeometry);
          if (targetGeometry == null) {
            continue;
          }
        } catch (Exception e) {
          continue;
        }

        final TileGeomResult tileGeom = JtsAdapter.createTileGeom(
            JtsAdapter.flatFeatureList(targetGeometry),
            tileEnvelope,
            clipEnvelope,
            geomFactory,
            layerParams,
            this);
        final List<Tile.Feature> features =
            JtsAdapter.toFeatures(tileGeom.mvtGeoms, layerProperties, process(feature));
        for (int j = 0; j < features.size(); j++) {
          layerBuilder.addFeatures(features.get(j));
        }
      }
    }

    // Finally create the tile.
    MvtLayerBuild.writeProps(layerBuilder, layerProperties);
    tileBuilder.addLayers(layerBuilder);
    VectorTile.Tile tile = tileBuilder.build();
    return tile.toByteArray();
  }

  private MvtTileBuilder process(final Feature feature) {
    this.feature = feature;
    return this;
  }

  /**
   * Exception handler to be called when an exception is raised while features are transformed from WGS'84 coordinates to the desired target
   * coordinate reference system. By default this method will simply throw the exception again, but when the exception should be ignore and
   * only this feature should be ignored, then this method can be overridden and the exception can be suppressed and e.g. logged.
   */
  protected void onTransformException(Exception e) throws Exception {
    // logger.info(tileQuery.http, e, "{}: Unexpected exception while transforming geometry, skip feature with id
    // '{}'",
    // Marshaller.class,
    // feature.id());
    throw e;
  }

  protected String newPrefix(final String prefix, String key) throws NullPointerException {
    if (key.indexOf('.') >= 0 || key.indexOf('~') >= 0) {
      key = key.replace("~", "~~").replace(".", "~");
    }
    if (prefix == null || prefix.length() == 0) {
      return key;
    }
    return prefix + "." + key;
  }

  protected void addProperty(final String prefix, final String key, final Object raw) {
    if (raw instanceof Boolean) {
      final boolean value = (Boolean) raw;
      final int keyIndex = layerProps.addKey(newPrefix(prefix, key));
      final int valueIndex = layerProps.addValue(value ? 1 : 0);
      featureBuilder.addTags(keyIndex);
      featureBuilder.addTags(valueIndex);
      return;
    }
    if ((raw instanceof String) || (raw instanceof Number)) {
      final int keyIndex = layerProps.addKey(newPrefix(prefix, key));
      final int valueIndex = layerProps.addValue(raw);
      featureBuilder.addTags(keyIndex);
      featureBuilder.addTags(valueIndex);
      return;
    }
  }

  /**
   * Returns the currently processed feature.
   */
  protected Feature currentFeature() {
    return this.feature;
  }

  @Override
  public boolean accept(com.vividsolutions.jts.geom.Geometry geometry) {
    return true;
  }

  public abstract void doAddTags(
      final Object userData,
      final MvtLayerProps layerProps,
      final VectorTile.Tile.Feature.Builder featureBuilder);

  @Override
  public void addTags(
      final Object userData,
      final MvtLayerProps layerProps,
      final VectorTile.Tile.Feature.Builder featureBuilder) {
    doAddTags(userData, layerProps, featureBuilder);
  }
}
