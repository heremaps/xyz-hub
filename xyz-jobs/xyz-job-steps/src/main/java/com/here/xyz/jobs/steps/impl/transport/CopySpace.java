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

package com.here.xyz.jobs.steps.impl.transport;

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.jobs.steps.execution.db.Database;
import com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole;
import com.here.xyz.jobs.steps.impl.SpaceBasedStep;
import com.here.xyz.jobs.steps.impl.tools.ResourceAndTimeCalculator;
import com.here.xyz.jobs.steps.impl.transport.query.ExportSpace;
import com.here.xyz.jobs.steps.impl.transport.query.ExportSpaceByGeometry;
import com.here.xyz.jobs.steps.impl.transport.query.ExportSpaceByProperties;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.query.SearchForFeatures;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.web.XyzWebClient.WebClientException;

import static com.here.xyz.jobs.steps.impl.transport.TransportTools.Phase.STEP_EXECUTE;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.createQueryContext;
import static com.here.xyz.jobs.steps.impl.transport.TransportTools.infoLog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole.WRITER;
import static com.here.xyz.jobs.steps.execution.db.Database.loadDatabase;
import static com.here.xyz.util.web.XyzWebClient.WebClientException;

/**
 * This step copies the content of a source space into a target space. It is possible that the target space
 * already contains data. Space where versioning is enabled are also supported. With filters it is possible to
 * only read a dataset subset from source space.
 *
 * @TODO
 * - onStateCheck
 * - resume
 * - provide output
 * - add i/o report
 * - move out parsePropertiesQuery functions
 */
public class CopySpace extends SpaceBasedStep<CopySpace> {
  private static final Logger logger = LogManager.getLogger();

  @JsonView({Internal.class, Static.class})
  private double overallNeededAcus = -1;

  @JsonView({Internal.class, Static.class})
  private long estimatedSourceFeatureCount = -1;

  @JsonView({Internal.class, Static.class})
  private long estimatedSourceByteSize = -1;

  @JsonView({Internal.class, Static.class})
  private long estimatedTargetFeatureCount = -1;

  @JsonView({Internal.class, Static.class})
  private int estimatedSeconds = -1;

  @JsonView({Internal.class, Static.class})
  private int[] threadInfo = {0,1};  // [threadId, threadCount]

  @JsonView({Internal.class, Static.class})
  private long version = 0; 

  //Existing Space in which we copy to
  private String targetSpaceId;

  //Geometry-Filters
  private Geometry geometry;
  private int radius = -1;
  private boolean clipOnFilterGeometry;

  //Content-Filters
  private PropertiesQuery propertyFilter;
  private Ref sourceVersionRef;

  public Geometry getGeometry() {
    return geometry;
  }

  public void setGeometry(Geometry geometry) {
    this.geometry = geometry;
  }

  public CopySpace withGeometry(Geometry geometry) {
    setGeometry(geometry);
    return this;
  }

  public int getRadius() {
    return radius;
  }

  public void setRadius(int radius) {
    this.radius = radius;
  }

  public CopySpace withRadius(int radius) {
    setRadius(radius);
    return this;
  }

  public boolean isClipOnFilterGeometry() {
    return clipOnFilterGeometry;
  }

  public void setClipOnFilterGeometry(boolean clipOnFilterGeometry) {
    this.clipOnFilterGeometry = clipOnFilterGeometry;
  }

  public CopySpace withClipOnFilterGeometry(boolean clipOnFilterGeometry) {
    setClipOnFilterGeometry(clipOnFilterGeometry);
    return this;
  }

  public PropertiesQuery getPropertyFilter() {
    return propertyFilter;
  }

  public void setPropertyFilter(PropertiesQuery propertyFilter) {
    this.propertyFilter = propertyFilter;
  }

  public CopySpace withPropertyFilter(PropertiesQuery propertyFilter){
    setPropertyFilter(propertyFilter);
    return this;
  }


  public Ref getSourceVersionRef() {
    return sourceVersionRef;
  }

  public void setSourceVersionRef(Ref sourceVersionRef) {
    this.sourceVersionRef = sourceVersionRef;
  }

  public CopySpace withSourceVersionRef(Ref sourceVersionRef) {
    setSourceVersionRef(sourceVersionRef);
    return this;
  }

  public String getTargetSpaceId() {
    return targetSpaceId;
  }

  public void setTargetSpaceId(String targetSpaceId) {
    this.targetSpaceId = targetSpaceId;
  }

  public CopySpace withTargetSpaceId(String targetSpaceId) {
    setTargetSpaceId(targetSpaceId);
    return this;
  }

  public int[] getThreadInfo() {
    return threadInfo;
  }

  public void setThreadInfo(int[] threadInfo) {
    this.threadInfo = threadInfo;
  }

  public CopySpace withThreadInfo(int[] threadInfo) {
    setThreadInfo( threadInfo );
    return this;
  }

  public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public CopySpace withVersion(long version) {
    setVersion(version);
    return this;
  }


  @Override
  public List<Load> getNeededResources() {
    try {
      List<Load> rList  = new ArrayList<>();
      Space sourceSpace = loadSpace(getSpaceId());
      Space targetSpace = loadSpace(getTargetSpaceId());

      rList.add( new Load().withResource(loadDatabase(targetSpace.getStorage().getId(), WRITER))
                           .withEstimatedVirtualUnits(calculateNeededAcus()) );

      boolean bRemoteCopy = isRemoteCopy(sourceSpace, targetSpace);                               

      if( bRemoteCopy )
       rList.add( new Load().withResource(loadDatabaseReaderElseWriter(sourceSpace.getStorage().getId()))
                            .withEstimatedVirtualUnits(calculateNeededAcus()) );

      logger.info("[{}] Copy remote({}) #{} {} -> {}", getGlobalStepId(), 
                                                           bRemoteCopy, rList.size(),
                                                           sourceSpace.getStorage().getId(), 
                                                           targetSpace.getStorage().getId() );

      return rList;                            
    }
    catch (WebClientException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getTimeoutSeconds() {
    
    return 24 * 3600;
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    if (estimatedSeconds == -1 && getSpaceId() != null) {

      estimatedSeconds = ResourceAndTimeCalculator.getInstance().calculateCopyTimeInSeconds(getSpaceId(), getTargetSpaceId(), 
                                                                                            estimatedSourceFeatureCount, estimatedTargetFeatureCount , 
                                                                                            getExecutionMode());
                                                                                            
      logger.info("[{}] Copy estimatedSeconds {}", getGlobalStepId(), estimatedSeconds);
    }
    return estimatedSeconds;
  }

  private double calculateNeededAcus() { 
    overallNeededAcus =  overallNeededAcus != -1 
                         ? overallNeededAcus 
                         : ResourceAndTimeCalculator.getInstance().calculateNeededCopyAcus(estimatedSourceFeatureCount);
    return overallNeededAcus;
  }

  @Override
  public String getDescription() {
    return "Copy space " + getSpaceId()+ " to space " +getTargetSpaceId();
  }

  @Override
  public boolean validate() throws ValidationException {
    super.validate();

    //@TODO: think about iml naming?
    if(getTargetSpaceId() == null)
      throw new ValidationException("Target Id is missing!");
    if(getSpaceId().equalsIgnoreCase(getTargetSpaceId()))
      throw new ValidationException("Source = Target!");

    try {
//???
      Space sourceSpace = loadSpace(getSpaceId());
      boolean isExtended = sourceSpace.getExtension() != null;
      StatisticsResponse sourceStatistics = loadSpaceStatistics(getSpaceId(), isExtended ? EXTENSION : null);
      estimatedSourceFeatureCount = sourceStatistics.getCount().getValue();
      estimatedSourceByteSize = sourceStatistics.getDataSize().getValue();
    }catch (WebClientException e) {
        throw new ValidationException("Error loading source space \"" + getSpaceId() + "\"", e);
    }

    try {
//???
      Space targetSpace = loadSpace(getSpaceId());
      boolean isExtended = targetSpace.getExtension() != null;
      StatisticsResponse targetStatistics = loadSpaceStatistics(getTargetSpaceId(), isExtended ? EXTENSION : null);
      estimatedTargetFeatureCount = targetStatistics.getCount().getValue();
    }catch (WebClientException e) {
      throw new ValidationException("Error loading target space \"" + getTargetSpaceId() + "\"", e);
    }

    if (estimatedSourceFeatureCount == 0)
      throw new ValidationException("Source Space is empty!");

    sourceVersionRef = sourceVersionRef == null ? new Ref("HEAD") : sourceVersionRef;

    if(sourceVersionRef.isTag()) {
      /** Resolve version Tag - if provided */
      try {
        long version = loadTag(getSpaceId(), sourceVersionRef.getTag()).getVersion();
        if (version >= 0) {
          sourceVersionRef = new Ref(version);
        }
      } catch (WebClientException e) {
        throw new ValidationException("Error resolving versionRef \"" + sourceVersionRef.getTag()+ "\" on " + getSpaceId(), e);
      }
    }

    return true;
  }

  public long setVersionToNextInSequence() throws SQLException, TooManyResourcesClaimed, WebClientException {

    Space targetSpace   = loadSpace(getTargetSpaceId());
    Database targetDb   = loadDatabase(targetSpace.getStorage().getId(), WRITER);
    String targetSchema = getSchema( targetDb ), 
            targetTable = _getRootTableName(targetSpace);

    SQLQuery incVersionSql = new SQLQuery("SELECT nextval('${schema}.${versionSequenceName}')")
                                  .withVariable("schema", targetSchema)
                                  .withVariable("versionSequenceName", targetTable + "_version_seq");

                                
    long newVersion = runReadQuerySync(incVersionSql, targetDb, 0, rs -> {
      rs.next();
      return rs.getLong(1);
    });

    setVersion( newVersion );
    return newVersion;
  }

  
  @Override
  public void execute() throws Exception {
    logger.info( "Loading space config for source-space "+getSpaceId());
    Space sourceSpace = loadSpace(getSpaceId());

    logger.info( "Loading space config for target-space " + getTargetSpaceId());
    Space targetSpace = loadSpace(getTargetSpaceId());

    logger.info("Getting storage database for space  "+getSpaceId());
    Database db = loadDatabase(targetSpace.getStorage().getId(), WRITER);

    int threadId = getThreadInfo()[0],
        threadCount = getThreadInfo()[1];

     infoLog(STEP_EXECUTE, this,String.format("Start ImlCopy thread number: %d/%d", threadId, threadCount )); 
     runReadQueryAsync(buildCopySpaceQuery(sourceSpace,targetSpace,threadCount, threadId), db, calculateNeededAcus()/threadCount, true);

  }

  @Override
  protected void onAsyncSuccess() throws WebClientException,
          SQLException, TooManyResourcesClaimed, IOException {

    logger.info("[{}] AsyncSuccess Copy {} -> {}", getGlobalStepId(), getSpaceId() , getTargetSpaceId());

  }

  @Override
  protected void onStateCheck() {
    //@TODO: Implement
    logger.info("Copy - onStateCheck");
    getStatus().setEstimatedProgress(0.2f);
  }

  @Override
  public void resume() throws Exception {
    //@TODO: Implement
    logger.info("Copy - onAsyncSuccess");
  }

  private String _getRootTableName(Space targetSpace) throws SQLException
  { try { return getRootTableName(targetSpace); } 
    catch (WebClientException e) {
      throw new SQLException(e);
    } 
  }

  private Database loadDatabaseReaderElseWriter(String name) 
  {
    try{ return loadDatabase(name,DatabaseRole.READER); }
    catch( RuntimeException rt )
    { if(!(rt.getCause() instanceof NoSuchElementException) )
       throw rt;
    }

    return loadDatabase(name,DatabaseRole.WRITER);
  }

  private boolean isRemoteCopy(Space sourceSpace, Space targetSpace)
  {
    String sourceStorage = sourceSpace.getStorage().getId(),
           targetStorage = targetSpace.getStorage().getId();
    return !sourceStorage.equals( targetStorage );
  }

  private boolean isRemoteCopy() throws WebClientException
  { Space sourceSpace = loadSpace(getSpaceId());
    Space targetSpace = loadSpace(getTargetSpaceId());
    return isRemoteCopy(sourceSpace, targetSpace);
  }

  private SQLQuery buildCopySpaceQuery(Space sourceSpace, Space targetSpace, int threadCount, int threadId) throws SQLException {

    String sourceStorageId = sourceSpace.getStorage().getId(),
           targetStorageId = targetSpace.getStorage().getId(), 
           targetSchema = getSchema( loadDatabase(targetStorageId, WRITER) ), 
           targetTable  = _getRootTableName(targetSpace);

    int maxBlkSize = 1000;

    final Map<String, Object> queryContext = 
      createQueryContext(getId(), 
                         targetSchema, 
                         targetTable, 
                         targetSpace.getVersionsToKeep() > 1, null);
    
    SQLQuery contentQuery = buildCopyContentQuery(sourceSpace, threadCount, threadId);

    if( isRemoteCopy(sourceSpace,targetSpace ) )
     contentQuery = buildCopyQueryRemoteSpace( loadDatabaseReaderElseWriter(sourceStorageId), contentQuery );
      
     SQLQuery versionToBeUsed = getVersion() > 0 ? new SQLQuery(""+getVersion())
                                                 : new SQLQuery("(SELECT nextval('${schema}.${versionSequenceName}'))")
                                                        .withVariable("schema", targetSchema)
                                                        .withVariable("versionSequenceName", targetTable + "_version_seq");
                   

    return new SQLQuery(
/**/
  """
    WITH ins_data as
    (
      select
        write_features(
         jsonb_build_array(
           jsonb_build_object('updateStrategy','{"onExists":null,"onNotExists":null,"onVersionConflict":null,"onMergeConflict":null}'::jsonb,
                              'partialUpdates',false,
                              'featureData', jsonb_build_object( 'type', 'FeatureCollection', 'features', jsonb_agg( iidata.feature ) )))::text
         ,iidata.author,false,${{versionToBeUsed}}
        ) as wfresult
      from
      (
       select ((row_number() over ())-1)/${{maxblksize}} as rn, idata.author, idata.jsondata || jsonb_build_object('geometry',st_asgeojson(idata.geo)::json) as feature
       from
       ( ${{contentQuery}} ) idata
      ) iidata
      group by rn, author
    )
    select sum((wfresult::json->>'count')::bigint)::bigint into dummy_output from ins_data
  """
/**/
        )
        .withContext( queryContext )
        .withQueryFragment("maxblksize",""+ maxBlkSize)
        .withQueryFragment("versionToBeUsed", versionToBeUsed )
        .withQueryFragment("contentQuery", contentQuery);
    
  }

  boolean _isEnableHashedSpaceIdActivated(Space space) throws SQLException
  {   
    try { return isEnableHashedSpaceIdActivated(space); } 
    catch (WebClientException e) {
      throw new SQLException(e);
    }
  }

  private SQLQuery buildCopyContentQuery(Space space, int threadCount, int threadId) throws SQLException {

    GetFeaturesByGeometryEvent event = new GetFeaturesByGeometryEvent()
            .withSpace(space.getId())
            //@TODO: verify if needed persistExport | versionsToKeep | context
            .withVersionsToKeep(space.getVersionsToKeep())
            .withRef(sourceVersionRef)
            .withContext(EXTENSION)
            .withPropertiesQuery(propertyFilter)
            .withConnectorParams(Collections.singletonMap("enableHashedSpaceId", _isEnableHashedSpaceIdActivated(space) ));

    if (geometry != null) {
      event.setGeometry(geometry);
      event.setClip(clipOnFilterGeometry);
      if(radius != -1) event.setRadius(radius);
    }

    if( threadCount <= 1 ) 
    { threadCount = 1; threadId = 0; }

    SQLQuery threadCondition = new SQLQuery(" AND (( i % #{threadCount} ) = #{threadNumber})")
        .withNamedParameter("threadCount", threadCount)
        .withNamedParameter("threadNumber", threadId);

    try {
      return ((ExportSpace) getQueryRunner(event))
              //TODO: Why not selecting the feature id / geo here?
              //FIXME: Do not select operation / author as part of the "property-selection"-fragment
              .withSelectionOverride(new SQLQuery("jsondata, author"))
              .withCustomWhereClause(threadCondition)
              .buildQuery(event);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  private SearchForFeatures getQueryRunner(GetFeaturesByGeometryEvent event) throws SQLException,
          ErrorResponseException, TooManyResourcesClaimed, WebClientException {
            
    Space sourceSpace = loadSpace(getSpaceId());

    Database db = !isRemoteCopy() 
     ? loadDatabase(sourceSpace.getStorage().getId(), WRITER )
     : loadDatabaseReaderElseWriter(sourceSpace.getStorage().getId());

    SearchForFeatures queryRunner;
    if (geometry == null)
      queryRunner = new ExportSpaceByProperties(event);
    else
      queryRunner = new ExportSpaceByGeometry(event);

    queryRunner.setDataSourceProvider(requestResource(db,0));
    return queryRunner;
  }

}
