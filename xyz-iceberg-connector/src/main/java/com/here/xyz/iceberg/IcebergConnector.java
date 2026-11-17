/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.iceberg;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.BINARY;
import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.GEO_JSON;
import static com.here.xyz.models.hub.IcebergLocation.EVENT_PARAM;
import static com.here.xyz.models.hub.IcebergLocation.EVENT_PARAM_LOCATION;
import static com.here.xyz.models.hub.IcebergLocation.ITERATE_TOKEN_PREFIX;
import static com.here.xyz.responses.XyzError.EXCEPTION;
import static com.here.xyz.responses.XyzError.ILLEGAL_ARGUMENT;
import static com.here.xyz.responses.XyzError.NOT_FOUND;
import static com.here.xyz.responses.XyzError.NOT_IMPLEMENTED;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.connectors.StorageConnector;
import com.here.xyz.events.DeleteChangesetsEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.GetStorageStatisticsEvent;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySubscriptionEvent;
import com.here.xyz.events.OneTimeActionEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQuery.QueryOperation;
import com.here.xyz.events.PropertyQueryList;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.SelectiveEvent;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.iceberg.FeatureScan.ColumnFilter;
import com.here.xyz.iceberg.FeatureScan.Position;
import com.here.xyz.iceberg.FeatureScan.Result;
import com.here.xyz.iceberg.IcebergTables.SnapshotFiles;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.IcebergLocation;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.StorageStatistics;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.geo.GeoTools;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.geotools.api.referencing.operation.MathTransform;
import org.geotools.geometry.jts.JTS;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

/**
 * A read-only connector for exported space versions stored in Apache Iceberg tables.
 * The Hub passes the location of the version ({@code iceberg://<namespace>/<table>?ref=<tag>}) in
 * {@code event.params.iceberg.location}; the catalog comes from the connector params (see {@link IcebergConnectorConfig}).
 * Unsupported events and options are answered with NOT_IMPLEMENTED, so the Hub can serve the request from the primary storage.
 * <p>
 * The connector reads whichever table of the catalog the location names. Whoever can set the location (in the Hub: the
 * {@code location} of an ICEBERG data reference) decides which data a space read returns, so only trusted callers may set it.
 */
public class IcebergConnector extends StorageConnector {

  private static final Logger logger = LogManager.getLogger(IcebergConnector.class);
  private static final String RAW_OPTIMIZATION_MODE = "raw";
  private static final int BUFFER_QUADRANT_SEGMENTS = 8;
  private static final double MIN_UTM_LATITUDE = -80;
  private static final double MAX_UTM_LATITUDE = 84;
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  private IcebergConnectorConfig config;
  private IcebergTables tables;

  @Override
  protected void initialize(Event event) throws Exception {
    try {
      config = IcebergConnectorConfig.from(event.getConnectorParams());
    }
    catch (IllegalArgumentException | IllegalStateException e) {
      throw new ErrorResponseException(EXCEPTION, "Invalid configuration of the Iceberg connector: " + e.getMessage());
    }
    try {
      tables = IcebergTables.forConfig(config);
    }
    catch (RuntimeException e) {
      logger.error("{} Unable to initialize the Iceberg catalog {}", traceItem, config.catalogName(), e);
      throw new ErrorResponseException(EXCEPTION, "Unable to initialize the Iceberg catalog " + config.catalogName());
    }
  }

  @Override
  protected FeatureCollection processIterateFeaturesEvent(IterateFeaturesEvent event) throws Exception {
    List<List<ColumnFilter>> filters = checkSearchOptions(event);
    if (event.getStartI() != -1 || event.getEndI() != -1)
      throw notImplemented("Partitioned iteration (startI / endI)");

    Long tokenSnapshotId = null;
    Position start = new Position(0, 0);
    if (event.getNextPageToken() != null) {
      PageToken token = PageToken.parse(event.getNextPageToken());
      tokenSnapshotId = token.snapshotId();
      start = token.position();
    }

    long startTime = System.currentTimeMillis();
    IcebergLocation location = location(event);
    SnapshotFiles files = tables.resolve(location);
    if (tokenSnapshotId != null && tokenSnapshotId != files.snapshotId())
      throw new ErrorResponseException(ILLEGAL_ARGUMENT, "The page token belongs to another version of the data");
    if (start.fileIndex() > files.tasks().size())
      throw new ErrorResponseException(ILLEGAL_ARGUMENT, "Invalid page token");

    FeatureScan scan = newScan(files, startTime).withFilters(filters).withLimit(event.getLimit()).withStart(start);
    Result result = run(event, location, files, scan, startTime);
    FeatureCollection response = response(result);
    if (result.next() != null) {
      String nextToken = new PageToken(files.snapshotId(), result.next()).toString();
      response.setHandle(nextToken); //Kept for backwards compatibility, like in the PSQL connector
      response.setNextPageToken(nextToken);
    }
    return response;
  }

  @Override
  protected FeatureCollection processGetFeaturesByBBoxEvent(GetFeaturesByBBoxEvent event) throws Exception {
    List<List<ColumnFilter>> filters = checkBBoxOptions(event);
    if (Boolean.TRUE.equals(event.getClip()))
      throw notImplemented("clip");
    return spatialRead(event, envelopeGeometry(event.getBbox()), filters);
  }

  @Override
  protected FeatureCollection processGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception {
    List<List<ColumnFilter>> filters = checkBBoxOptions(event);
    if (event.getResponseType() == BINARY)
      throw notImplemented("Binary tiles");
    //MVT tiles are clipped by the Hub when it transforms the GeoJSON response
    if ((event.getResponseType() == null || event.getResponseType() == GEO_JSON) && Boolean.TRUE.equals(event.getClip()))
      throw notImplemented("clip");
    //The bbox of the event already contains the margin
    return spatialRead(event, envelopeGeometry(event.getBbox()), filters);
  }

  @Override
  protected FeatureCollection processGetFeaturesByGeometryEvent(GetFeaturesByGeometryEvent event) throws Exception {
    List<List<ColumnFilter>> filters = checkSearchOptions(event);
    if (Boolean.TRUE.equals(event.getClip()))
      throw notImplemented("clip");
    if (event.getH3Index() != null)
      throw notImplemented("h3Index");
    if (event.getGeometry() == null)
      throw new ErrorResponseException(ILLEGAL_ARGUMENT, "The geometry is missing");
    Geometry geometry = event.getGeometry().getJTSGeometry();
    if (event.getRadius() > 0)
      geometry = buffer(geometry, event.getRadius());
    return spatialRead(event, geometry, filters);
  }

  @Override
  protected FeatureCollection processSearchForFeaturesEvent(SearchForFeaturesEvent event) throws Exception {
    List<List<ColumnFilter>> filters = checkSearchOptions(event);
    long startTime = System.currentTimeMillis();
    IcebergLocation location = location(event);
    SnapshotFiles files = tables.resolve(location);
    FeatureScan scan = newScan(files, startTime).withFilters(filters).withLimit(event.getLimit());
    return response(run(event, location, files, scan, startTime));
  }

  private FeatureCollection spatialRead(SearchForFeaturesEvent<?> event, Geometry geometry, List<List<ColumnFilter>> filters)
      throws Exception {
    long startTime = System.currentTimeMillis();
    IcebergLocation location = location(event);
    SnapshotFiles files = tables.resolve(location);
    FeatureScan scan = newScan(files, startTime).withGeometry(geometry).withFilters(filters).withLimit(event.getLimit());
    return response(run(event, location, files, scan, startTime));
  }

  private FeatureScan newScan(SnapshotFiles files, long startTime) {
    return new FeatureScan(tables, files)
        .withBudget(new ReadBudget(config.maxBytesPerRequest(), startTime + config.requestTimeoutMs()))
        .withMaxResponseBytes(config.maxResponseBytes());
  }

  private Result run(Event<?> event, IcebergLocation location, SnapshotFiles files, FeatureScan scan, long startTime) {
    Result result = null;
    try {
      result = scan.run();
      return result;
    }
    finally {
      FeatureScan.Stats stats = scan.stats();
      logger.info("{} iceberg-read event={} location={} snapshot={} filesPlanned={} filesPruned={} filesRead={} bytesRead={} "
              + "rowsScanned={} rowsMatched={} ms={} outcome={}", traceItem, event.getClass().getSimpleName(), location,
          files.snapshotId(), stats.filesPlanned, stats.filesPruned, stats.filesRead, scan.bytesRead(), stats.rowsScanned,
          stats.rowsMatched, System.currentTimeMillis() - startTime, result != null ? "ok" : "error");
    }
  }

  private static FeatureCollection response(Result result) {
    FeatureCollection response = new FeatureCollection();
    response._setFeatures("[" + String.join(",", result.features()) + "]");
    return response;
  }

  private static IcebergLocation location(Event<?> event) throws ErrorResponseException {
    Object icebergParams = event.getParams() == null ? null : event.getParams().get(EVENT_PARAM);
    Object location = icebergParams instanceof Map<?, ?> map ? map.get(EVENT_PARAM_LOCATION) : null;
    if (!(location instanceof String locationString))
      throw new ErrorResponseException(ILLEGAL_ARGUMENT, "The Iceberg location is missing in params." + EVENT_PARAM + "."
          + EVENT_PARAM_LOCATION);
    try {
      return IcebergLocation.parse(locationString);
    }
    catch (IllegalArgumentException e) {
      throw new ErrorResponseException(ILLEGAL_ARGUMENT, e.getMessage());
    }
  }

  private List<List<ColumnFilter>> checkBBoxOptions(GetFeaturesByBBoxEvent<?> event) throws ErrorResponseException {
    List<List<ColumnFilter>> filters = checkSearchOptions(event);
    if (event.getClusteringType() != null)
      throw notImplemented("clustering");
    if (event.getTweakType() != null)
      throw notImplemented("tweaks");
    if (event.getOptimizationMode() != null && !RAW_OPTIMIZATION_MODE.equals(event.getOptimizationMode()))
      throw notImplemented("optimizationMode " + event.getOptimizationMode());
    BBox bbox = event.getBbox();
    if (bbox == null)
      throw new ErrorResponseException(ILLEGAL_ARGUMENT, "The bbox is missing");
    if (bbox.minLon() > bbox.maxLon())
      throw notImplemented("A bbox crossing the antimeridian");
    return filters;
  }

  /**
   * Checks the options all supported reads share and returns the property filters (null if there are none).
   */
  private List<List<ColumnFilter>> checkSearchOptions(SearchForFeaturesEvent<?> event) throws ErrorResponseException {
    checkSelectiveOptions(event);
    return filters(event.getPropertiesQuery(), config.filterColumns());
  }

  private static void checkSelectiveOptions(SelectiveEvent<?> event) throws ErrorResponseException {
    if (event.getSelection() != null && !event.getSelection().isEmpty())
      throw notImplemented("selection");
    if (event.isForce2D())
      throw notImplemented("force2D");
    if (event.getContext() != DEFAULT)
      throw notImplemented("Context " + event.getContext());
  }

  /**
   * Converts a properties query that only filters by the configured filter properties (EQUALS with String values) into an OR of
   * AND lists of column value sets.
   */
  static List<List<ColumnFilter>> filters(PropertiesQuery propertiesQuery, Map<String, String> filterColumns)
      throws ErrorResponseException {
    if (propertiesQuery == null || propertiesQuery.isEmpty())
      return null;
    List<List<ColumnFilter>> anyOf = new ArrayList<>();
    for (PropertyQueryList andList : propertiesQuery) {
      if (andList == null || andList.isEmpty())
        throw notImplemented("An empty properties query");
      List<ColumnFilter> allOf = new ArrayList<>();
      for (PropertyQuery query : andList) {
        String column = filterColumns.get(query.getKey());
        if (column == null || query.getOperation() != QueryOperation.EQUALS || query.getValues() == null
            || query.getValues().isEmpty())
          throw notImplemented("Property queries other than equals on " + filterColumns.keySet());
        Set<String> values = new LinkedHashSet<>();
        for (Object value : query.getValues()) {
          if (!(value instanceof String stringValue))
            throw notImplemented("Non-string values of " + query.getKey());
          values.add(stringValue);
        }
        allOf.add(new ColumnFilter(column, values));
      }
      anyOf.add(allOf);
    }
    return anyOf;
  }

  private static Geometry envelopeGeometry(BBox bbox) {
    return GEOMETRY_FACTORY.toGeometry(new Envelope(bbox.minLon(), bbox.maxLon(), bbox.minLat(), bbox.maxLat()));
  }

  /**
   * Buffers the geometry by the radius in meters, in the UTM zone of its centroid.
   */
  static Geometry buffer(Geometry geometry, int radius) throws Exception {
    Point centroid = geometry.getCentroid();
    double lon = centroid.getX(), lat = centroid.getY();
    if (lat < MIN_UTM_LATITUDE || lat > MAX_UTM_LATITUDE)
      throw notImplemented("A radius query outside of the UTM latitudes");
    int zone = Math.min(60, Math.max(1, (int) Math.floor((lon + 180) / 6) + 1));
    String utm = "EPSG:" + ((lat >= 0 ? 32600 : 32700) + zone);
    MathTransform toUtm = GeoTools.mathTransform(GeoTools.WGS84_EPSG, utm);
    MathTransform fromUtm = GeoTools.mathTransform(utm, GeoTools.WGS84_EPSG);
    return JTS.transform(JTS.transform(geometry, toUtm).buffer(radius, BUFFER_QUADRANT_SEGMENTS), fromUtm);
  }

  private static ErrorResponseException notImplemented(String what) {
    return new ErrorResponseException(NOT_IMPLEMENTED, what + " is not supported by the Iceberg connector");
  }

  @Override
  protected void handleProcessingException(Exception exception, Event event) throws Exception {
    if (exception instanceof ErrorResponseException)
      throw exception;
    for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
      if (cause instanceof ScanException scanException)
        throw new ErrorResponseException(scanException.error(), scanException.getMessage());
      if (cause instanceof NoSuchTableException || cause instanceof NoSuchNamespaceException || cause instanceof NotFoundException)
        throw new ErrorResponseException(NOT_FOUND, cause.getMessage());
    }
    if (exception instanceof IllegalArgumentException)
      throw new ErrorResponseException(ILLEGAL_ARGUMENT, exception.getMessage());
    throw exception;
  }

  @Override
  protected StatisticsResponse processGetStatistics(GetStatisticsEvent event) throws Exception {
    throw notImplemented(event.getClass().getSimpleName());
  }

  @Override
  protected FeatureCollection processGetFeaturesByIdEvent(GetFeaturesByIdEvent event) throws Exception {
    throw notImplemented(event.getClass().getSimpleName());
  }

  @Override
  protected FeatureCollection processLoadFeaturesEvent(LoadFeaturesEvent event) throws Exception {
    throw notImplemented(event.getClass().getSimpleName());
  }

  @Override
  protected FeatureCollection processModifyFeaturesEvent(ModifyFeaturesEvent event) throws Exception {
    throw notImplemented(event.getClass().getSimpleName());
  }

  @Override
  protected FeatureCollection processWriteFeaturesEvent(WriteFeaturesEvent event) throws Exception {
    throw notImplemented(event.getClass().getSimpleName());
  }

  @Override
  protected SuccessResponse processModifySpaceEvent(ModifySpaceEvent event) throws Exception {
    throw notImplemented(event.getClass().getSimpleName());
  }

  @Override
  protected SuccessResponse processModifySubscriptionEvent(ModifySubscriptionEvent event) throws Exception {
    throw notImplemented(event.getClass().getSimpleName());
  }

  @Override
  protected StorageStatistics processGetStorageStatisticsEvent(GetStorageStatisticsEvent event) throws Exception {
    throw notImplemented(event.getClass().getSimpleName());
  }

  @Override
  protected XyzResponse processOneTimeActionEvent(OneTimeActionEvent event) throws Exception {
    throw notImplemented(event.getClass().getSimpleName());
  }

  @Override
  protected SuccessResponse processDeleteChangesetsEvent(DeleteChangesetsEvent event) throws Exception {
    throw notImplemented(event.getClass().getSimpleName());
  }

  @Override
  protected XyzResponse processIterateChangesetsEvent(IterateChangesetsEvent event) throws Exception {
    throw notImplemented(event.getClass().getSimpleName());
  }

  @Override
  protected ChangesetsStatisticsResponse processGetChangesetsStatisticsEvent(GetChangesetStatisticsEvent event) throws Exception {
    throw notImplemented(event.getClass().getSimpleName());
  }

  /**
   * An iterate page token: {@code ice:<snapshotId>:<fileIndex>:<rowPosition>}, where the row position is the position in the data
   * file of the first row which was not returned yet.
   */
  record PageToken(long snapshotId, Position position) {

    static PageToken parse(String token) throws ErrorResponseException {
      if (!token.startsWith(ITERATE_TOKEN_PREFIX))
        throw new ErrorResponseException(ILLEGAL_ARGUMENT, "Invalid page token");
      String[] parts = token.substring(ITERATE_TOKEN_PREFIX.length()).split(":", -1);
      if (parts.length != 3)
        throw new ErrorResponseException(ILLEGAL_ARGUMENT, "Invalid page token");
      try {
        long snapshotId = Long.parseLong(parts[0]);
        int fileIndex = Integer.parseInt(parts[1]);
        long rowPosition = Long.parseLong(parts[2]);
        if (fileIndex < 0 || rowPosition < 0)
          throw new ErrorResponseException(ILLEGAL_ARGUMENT, "Invalid page token");
        return new PageToken(snapshotId, new Position(fileIndex, rowPosition));
      }
      catch (NumberFormatException e) {
        throw new ErrorResponseException(ILLEGAL_ARGUMENT, "Invalid page token");
      }
    }

    @Override
    public String toString() {
      return ITERATE_TOKEN_PREFIX + snapshotId + ":" + position.fileIndex() + ":" + position.rowPosition();
    }
  }
}
