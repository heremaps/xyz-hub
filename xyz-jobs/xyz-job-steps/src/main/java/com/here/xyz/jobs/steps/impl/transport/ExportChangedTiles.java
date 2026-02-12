/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.jobs.steps.impl.transport;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import static com.here.xyz.jobs.steps.Step.Visibility.SYSTEM;
import static com.here.xyz.jobs.steps.Step.Visibility.USER;
import static com.here.xyz.jobs.steps.impl.SpaceBasedStep.LogPhase.STEP_EXECUTE;
import static com.here.xyz.jobs.steps.impl.SpaceBasedStep.LogPhase.STEP_ON_ASYNC_SUCCESS;
import static com.here.xyz.util.web.XyzWebClient.WebClientException;

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.util.datasets.filters.SpatialFilter;
import com.here.xyz.jobs.steps.StepExecution;
import com.here.xyz.jobs.steps.impl.transport.tasks.inputs.ExportInput;
import com.here.xyz.jobs.steps.impl.transport.tasks.outputs.ExportOutput;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.outputs.TileInvalidations;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.models.geojson.HQuad;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.coordinates.LinearRingCoordinates;
import com.here.xyz.models.geojson.coordinates.PolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.geojson.implementation.Polygon;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.psql.query.GetFeaturesByGeometryBuilder;
import com.here.xyz.psql.query.GetFeaturesByGeometryBuilder.GetFeaturesByGeometryInput;
import com.here.xyz.psql.query.GetFeaturesByIdsBuilder;
import com.here.xyz.psql.query.GetFeaturesByIdsBuilder.GetFeaturesByIdsInput;
import com.here.xyz.psql.query.QueryBuilder.QueryBuildingException;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;


/**
 * The {@code ExportChangedTiles} class extends {@link ExportSpaceToFiles} to specifically handle the
 * export of changed tiles from a given space. It determines the affected tiles based on changes
 * detected between different versions of spatial data.
 *
 * <p>Key features of this class include:
 * <ul>
 *   <li>Identifies affected tiles by analyzing spatial data changes across versions.</li>
 *   <li>Supports configurable target tile levels for export granularity.</li>
 *   <li>Handles tile representation using different quad types (HERE_QUAD, MERCATOR_QUAD).</li>
 *   <li>Ensures data consistency by validating version history and spatial constraints.</li>
 *   <li>Manages parallel execution for efficient processing of tile exports.</li>
 * </ul>
 * </p>
 *
 * <p>This step produces outputs of type {@link FeatureStatistics}, {@link DownloadUrl},
 * and {@link TileInvalidations}.</p>
 */
public class ExportChangedTiles extends ExportSpaceToFiles {
  //GetFeaturesById queries are getting chunked  in MAX_ID_BLOCKSIZE blocks
  private static final int MAX_ID_BLOCKSIZE = 400_000;
  public static final String TILE_INVALIDATIONS = "tileInvalidations";

  @JsonView({Internal.class, Static.class})
  private int targetLevel = 11;

  @JsonView({Internal.class, Static.class})
  private QuadType quadType = QuadType.HERE_QUAD;

  @JsonView({Internal.class, Static.class})
  private boolean clipOnTileBoundaries;

  public boolean isClipOnTileBoundaries() {
    return clipOnTileBoundaries;
  }

  public void setClipOnTileBoundaries(boolean clipOnTileBoundaries) {
    this.clipOnTileBoundaries = clipOnTileBoundaries;
  }

  public ExportChangedTiles withClipOnTileBoundaries(boolean clipOnTileBoundaries) {
    setClipOnTileBoundaries(clipOnTileBoundaries);
    return this;
  }

  public int getTargetLevel() {
    return targetLevel;
  }

  public void setTargetLevel(int targetLevel) {
    this.targetLevel = targetLevel;
  }

  public ExportChangedTiles withTargetLevel(int targetLevel) {
    setTargetLevel(targetLevel);
    return this;
  }

  public QuadType getQuadType() {
    return quadType;
  }

  public void setQuadType(QuadType quadType) {
    this.quadType = quadType;
  }

  public ExportChangedTiles withQuadType(QuadType quadType) {
    setQuadType(quadType);
    return this;
  }

  public enum QuadType {
    HERE_QUAD,
    MERCATOR_QUAD
  }

  {
    threadCount = 8;
    setOutputSets(List.of(
        new OutputSet(STATISTICS, USER, true),
        new OutputSet(EXPORTED_DATA, USER, false),
        new OutputSet(TILE_INVALIDATIONS, SYSTEM, true)
    ));
  }

  @Override
  public boolean isEquivalentTo(StepExecution other) {
    if (!(other instanceof ExportChangedTiles otherExport))
      return super.isEquivalentTo(other);

    try {
      //Deduplicate code
      return Objects.equals(otherExport.getSpaceId(), getSpaceId())
              && Objects.equals(otherExport.versionRef, versionRef)
              && (otherExport.context == context || (space().getExtension() == null && otherExport.context == null && context == SUPER))
              && Objects.equals(otherExport.spatialFilter, spatialFilter)
              && Objects.equals(otherExport.propertyFilter, propertyFilter)
              && Objects.equals(otherExport.targetLevel, targetLevel)
              && Objects.equals(otherExport.quadType, quadType)
              && Objects.equals(otherExport.spaceCreatedAt, spaceCreatedAt);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

// TODO: Fix and implement
//  @Override
//  public int getEstimatedExecutionSeconds() {
//  }

  @Override
  public String getDescription() {
    return "Export ChangedTiles from space " + getSpaceId();
  }

  @Override
  public boolean validate() throws ValidationException {
    //Disable spatial filter restriction
    restrictExtendOfSpatialFilter = false;

    super.validate();

    if (targetLevel < 0 || targetLevel > 12) {
      throw new ValidationException("TargetLevel must be between 0 and 12!");
    }
    return true;
  }

  @Override
  protected List<ExportInput> createTaskItems() throws TooManyResourcesClaimed,
          QueryBuildingException, WebClientException, SQLException {

    Set<String> affectedTiles = new HashSet<>();
    List<String> changedFeatureIds = new ArrayList<>();

    //Temporary evil hack!
    if(getSpaceId().contains(":")) {
      String c = getSpaceId().substring(0, getSpaceId().lastIndexOf(":"));
      String l = getSpaceId().substring(getSpaceId().lastIndexOf(":") + 1);

      if (c.equalsIgnoreCase("hrn:here:data::olp-here:mm-1138910520110-livemap-1765440871131")) {
        List<ExportInput> taskList = new ArrayList<>();

        return runReadQuerySync(new SQLQuery("SELECT tile FROM ${schema}.${table};")
                        .withVariable("schema", "tmp")
                        .withVariable("table", l + "_tileids"),
                db(), 0, rs -> {
                  while (rs.next()) {
                    String tileId = rs.getString("tile");
                    if (tileIsRelevant(tileId))
                      taskList.add(new ExportInput(tileId));
                  }
                  return taskList;
                });
      }
    }

    //Get affected Tiles from Delta in range [version.getStartVersion() + 1 : version.getEndVersion()]
    runReadQuerySync(getAffectedTilesFromDelta(new Ref(versionRef.getStart().getVersion(),
            versionRef.getEnd().getVersion())), db(), 0, rs -> {
      while (rs.next()){
        String tileID = rs.getString("tile");
        if(tileID != null)
          affectedTiles.add(tileID);
        changedFeatureIds.add(rs.getString("id"));
      }
      return null;
    });
    infoLog(STEP_EXECUTE,  "Added affected tiles from delta in version range "
            + versionRef +". Intermediate result size: "+ affectedTiles.size());

    if(!changedFeatureIds.isEmpty()){
      final int totalIdSize = changedFeatureIds.size();

      //Get affected Tiles from list of Feature in version [versionRef.getStartVersion()]
      for (int from = 0; from < totalIdSize; from += MAX_ID_BLOCKSIZE) {
        int to = Math.min(from + MAX_ID_BLOCKSIZE, totalIdSize);
        List<String> idBlock = changedFeatureIds.subList(from, to);

        runReadQuerySync(getAffectedTilesFromVersion(idBlock, new Ref(versionRef.getStart().getVersion())),
                db(), 0, rs -> {
                  while (rs.next()){
                    affectedTiles.add(rs.getString("tile"));
                  }
                  return null;
                });

        infoLog(STEP_EXECUTE,
                "Processed ID block [" + from + " - " + (to - 1) + "] Block size: " + idBlock.size()
                        + ", Current affectedTiles size: " + affectedTiles.size());
      }

      infoLog(STEP_EXECUTE,  "Added affected tiles from base version "
              + versionRef.getStart().getVersion() +". Final Result size: "+ affectedTiles.size());
    }

    List<ExportInput> taskList = new ArrayList<>();
    //Write taskList with all unique tileIds which we need to export
    for(String tileId : affectedTiles){
      if(!tileIsRelevant(tileId))
        continue;
      taskList.add(new ExportInput(tileId));
    }
    return taskList;
  }

  private boolean tileIsRelevant(String tileId){
    //If a spatialFilter is set with clip=true, we need to check if the tile intersects with the filter
    //We only add the tile if it intersects with the filter
    if(spatialFilter != null && spatialFilter.getGeometry() != null && spatialFilter.isClip())
      return spatialFilter.getGeometry().getJTSGeometry()
              .intersects(getTileBBOX(tileId).getJTSGeometry());
    return true;
  }

  private Polygon getTileBBOX(String tileId) {
    BBox tileBBOX = switch (quadType) {
      case HERE_QUAD -> new HQuad(tileId, false).getBoundingBox();
      case MERCATOR_QUAD -> WebMercatorTile.forQuadkey(tileId).getExtendedBBox(0);
    };

    PolygonCoordinates polygonCoordinates = new PolygonCoordinates();
    LinearRingCoordinates lrc = new LinearRingCoordinates();

    lrc.add(new Position(tileBBOX.minLon(), tileBBOX.minLat()));
    lrc.add(new Position(tileBBOX.minLon(), tileBBOX.maxLat()));
    lrc.add(new Position(tileBBOX.maxLon(), tileBBOX.maxLat()));
    lrc.add(new Position(tileBBOX.maxLon(), tileBBOX.minLat()));
    lrc.add(new Position(tileBBOX.minLon(), tileBBOX.minLat()));
    polygonCoordinates.add(lrc);
    return new Polygon().withCoordinates(polygonCoordinates);
  }

  @Override
  protected String generateContentQueryForExportPlugin(ExportInput taskInput)
          throws WebClientException, TooManyResourcesClaimed, QueryBuildingException, InvalidGeometryException {
    //We are exporting now the data from the provided tile ID.
    return getFeaturesByTileIdQuery(
            DEFAULT,
            null, //no override needed - use default
            spatialFilter, //no spatial Filter is needed - we take Geometry from tile
            taskInput.tileId(), //tileId from task_item
            new Ref(versionRef.getEnd().getVersion()) //export tiles from endVersion
    ).toExecutableQueryString();
  }

  @Override
  protected void processFinalizedTasks(List<FinalizedTaskItem<ExportInput, ExportOutput>> finalizedTaskItems) throws IOException {
    try {
      List<String> invalidatedTileIds = new ArrayList<>();

      for(FinalizedTaskItem<ExportInput, ExportOutput> item : finalizedTaskItems){
        if(item.output().bytes() == 0)
          invalidatedTileIds.add(item.input().tileId());
      }

      TileInvalidations tileList = new TileInvalidations()
              .withTileLevel(targetLevel)
              .withQuadType(quadType)
              .withTileIds(invalidatedTileIds);

      infoLog(STEP_ON_ASYNC_SUCCESS,  "Write TILE_INVALIDATIONS output. Size: {}.",
              Integer.toString(tileList.getTileIds().size()));

      registerOutputs(List.of(tileList), TILE_INVALIDATIONS);

    } catch (Exception e) {
      throw new IOException(e);
    }
    super.processFinalizedTasks(finalizedTaskItems);
  }

  @Override
  protected void finalCleanUp(boolean noTasksCreated) throws WebClientException, SQLException, TooManyResourcesClaimed, IOException {
    if(noTasksCreated){
      //@TODO: Remove this if EMR is capable of handling not existing files
      registerOutputs(List.of(new DownloadUrl()
              .withContent(new byte[]{})
              .withFileName("empty")
      ), EXPORTED_DATA);
      registerOutputs(List.of(new TileInvalidations()
              .withTileLevel(targetLevel)
              .withQuadType(quadType)
              .withTileIds(new ArrayList<>())
      ), TILE_INVALIDATIONS);
    }
  }

  @Override
  protected long getEstimatedIOBytes() {
    // replace with real data usage
    return ESTIMATED_SPATIAL_FILTERED_IO_BYTES;
  }

  private String getQuadFunctionName(){
    return switch (quadType) {
      case HERE_QUAD -> "here_quad";
      case MERCATOR_QUAD -> "mercator_quad";
    };
  }

  private SQLQuery getAffectedTilesFromVersion(List<String> ids, Ref versionRef) throws TooManyResourcesClaimed, QueryBuildingException, WebClientException {
    SQLQuery getBaseFeaturesByIdQuery = generateGetFeaturesByIdsQuery(ids, DEFAULT, versionRef);

    SQLQuery getChangedTilesQuery =  new SQLQuery("SELECT "+getQuadFunctionName()+
            """
            (f.colX, f.rowY, f.level) as tile
               FROM (
                    ${{squashedDeltaChangesQuery}}
                ) a
            CROSS JOIN LATERAL for_geometry(a.geo, #{targetLevel}, #{quadType}) f
        """)
            .withNamedParameter("targetLevel", targetLevel)
            .withNamedParameter("quadType", quadType.name())
            .withQueryFragment("squashedDeltaChangesQuery", getBaseFeaturesByIdQuery)
            .withLoggingEnabled(false);;

    return getChangedTilesQuery;
  }

  private SQLQuery getAffectedTilesFromDelta(Ref versionRef)
          throws TooManyResourcesClaimed, QueryBuildingException, WebClientException {
    SQLQuery getDeltaFeaturesQuery = getFeaturesByGeometryQuery(
            EXTENSION,
            new SQLQuery("id, geo"),
            spatialFilter,
            versionRef
    );
    //We are getting a list tileId,id
    //If feature is deleted we do not have geometry anymore so we
    //in this case a row only have the id. cE.g. null,id123
    return new SQLQuery("SELECT "+getQuadFunctionName()+
            """
            (f.colX, f.rowY, f.level) as tile, id
            FROM (
                 ${{squashedDeltaChangesQuery}}
             ) a
            LEFT JOIN LATERAL for_geometry(a.geo, #{targetLevel}, #{quadType}) f ON TRUE;
     """)
            .withNamedParameter("targetLevel", targetLevel)
            .withNamedParameter("quadType", quadType.name())
            .withQueryFragment("squashedDeltaChangesQuery", getDeltaFeaturesQuery)
            .withLoggingEnabled(false);
  }

  private SQLQuery getFeaturesByGeometryQuery(
          SpaceContext context,
          SQLQuery selectClauseOverride,
          SpatialFilter spatialFilter,
          Ref versionRef
  ) throws WebClientException, TooManyResourcesClaimed, QueryBuildingException {

    GetFeaturesByGeometryBuilder queryBuilder = new GetFeaturesByGeometryBuilder()
            .withDataSourceProvider(requestResource(dbReader(), 0));

    GetFeaturesByGeometryInput input = createGetFeaturesByGeometryInput(context, spatialFilter, versionRef);

    return queryBuilder
            .withSelectClauseOverride(selectClauseOverride)
            .buildQuery(input);
  }

  private SQLQuery getFeaturesByTileIdQuery(
          SpaceContext context,
          SQLQuery selectClauseOverride,
          SpatialFilter spatialFilter,
          String tileId,
          Ref versionRef
  ) throws WebClientException, TooManyResourcesClaimed, QueryBuildingException, InvalidGeometryException {

    GetFeaturesByGeometryBuilder queryBuilder = new GetFeaturesByGeometryBuilder()
            .withDataSourceProvider(requestResource(dbReader(), 0));

    SpatialFilter tileBBOXFilter = new SpatialFilter()
            .withGeometry(getTileBBOX(tileId))
            .withClip(clipOnTileBoundaries);

    GetFeaturesByGeometryInput input = createGetFeaturesByGeometryInput(context, tileBBOXFilter, versionRef);

    SQLQuery contentQuery = queryBuilder
            .withClippingGeometry(spatialFilter != null && spatialFilter.isClip() ? spatialFilter.getGeometry() : null)
            .withSelectClauseOverride(selectClauseOverride)
            .buildQuery(input);

    return buildTileQuery(contentQuery, tileId);
  }

  private SQLQuery buildTileQuery(SQLQuery contentQuery, String tileId) {
    return new SQLQuery("""
          SELECT geo, jsonb_set(
                    jsondata,
                    '{properties,@ns:com:here:xyz,partitionKey}',
                    #{tileId}
                ) AS jsondata
             FROM (${{contentQuery}}) A
        """)
            .withNamedParameter("tileId", tileId)
            .withQueryFragment("contentQuery", contentQuery);
  }

  private SQLQuery generateGetFeaturesByIdsQuery(List<String> ids, SpaceContext context, Ref versionRef) throws WebClientException,
      TooManyResourcesClaimed, QueryBuildingException {
    GetFeaturesByIdsBuilder queryBuilder = new GetFeaturesByIdsBuilder().withSelectClauseOverride(new SQLQuery("id,geo"))
        .withDataSourceProvider(requestResource(dbReader(), 0));

    return queryBuilder.buildQuery(new GetFeaturesByIdsInput(
        space().getId(),
        hubWebClient().loadConnector(space().getStorage().getId()).params,
        space().getExtension() != null ? space().resolveCompositeParams(superSpace()) : null,
        context,
        space().getVersionsToKeep(),
        versionRef,
        ids
    )).withLoggingEnabled(false);
  }
}
