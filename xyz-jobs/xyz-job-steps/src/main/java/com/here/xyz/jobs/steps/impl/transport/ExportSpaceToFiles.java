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
import com.here.xyz.jobs.steps.impl.SpaceBasedStep;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.resources.IOResource;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.UUID;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.util.web.XyzWebClient.WebClientException;

/**
 * This step imports a set of user provided inputs and imports their data into a specified space.
 * This step produces exactly one output of type {@link FeatureStatistics}.
 */
public class ExportSpaceToFiles extends SpaceBasedStep<ExportSpaceToFiles> {
  private static final Logger logger = LogManager.getLogger();

  @JsonView({Internal.class, Static.class})
  private int calculatedThreadCount = -1;

  @JsonView({Internal.class, Static.class})
  private double overallNeededAcus = -1;

  @JsonView({Internal.class, Static.class})
  private EntityPerLine entityPerLine = EntityPerLine.Feature;

  private Format format = Format.GEOJSON;
  private Phase phase;

//  //Geometry-Filters
//  private Geometry geometry;
//  private int radius = -1;
//  private boolean clipOnFilterGeometry;
//
//  //Content-Filters
//  private String propertyFilter;
//  private SpaceContext context;
//  private String targetVersion;
//
//  //Partitioning
//  private String partitionKey;
//  //Required if partitionKey=tileId
//  private Integer targetLevel;
//  private boolean clipOnPartitions;

  public enum Format {
    CSV_JSON_WKB,
    CSV_PARTITIONED_JSON_WKB,
    GEOJSON;
  }

  public enum Phase {
    VALIDATE
  }

  public void setFormat(Format format) {
    this.format = format;
  }

  public ExportSpaceToFiles withFormat(Format format) {
    setFormat(format);
    return this;
  }

  public EntityPerLine getEntityPerLine() {
    return entityPerLine;
  }

  public void setEntityPerLine(EntityPerLine entityPerLine) {
    this.entityPerLine = entityPerLine;
  }

  public ExportSpaceToFiles withEntityPerLine(EntityPerLine entityPerLine) {
    setEntityPerLine(entityPerLine);
    return this;
  }

  public Phase getPhase() {
    return phase;
  }

  @JsonView({Internal.class, Static.class})
  private StatisticsResponse statistics = null;

  @Override
  public List<Load> getNeededResources() {
    try {
      overallNeededAcus = 10;
      return List.of(new Load().withResource(db()).withEstimatedVirtualUnits(overallNeededAcus),
              new Load().withResource(IOResource.getInstance()).withEstimatedVirtualUnits(getUncompressedUploadBytesEstimation()));
    }catch (Exception e){
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getTimeoutSeconds() {
    return 24 * 3600;
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    return 0;
  }

  @Override
  public String getDescription() {
    return "Export data from space " + getSpaceId();
  }

  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }

  @Override
  public boolean validate() throws ValidationException {
    super.validate();
    try {
      logger.info("VALIDATE");
      loadSpace(getSpaceId());
      statistics = loadSpaceStatistics(getSpaceId(), EXTENSION);
      long featureCount = statistics.getCount().getValue();

      /**
       * @TODO:
       * - Check if geometry is valid
       * - Check searchableProperties
       * - Check if targetVersion is valid
       * - Check if targetLevel is valid
      */
    }
    catch (WebClientException e) {
      throw new ValidationException("Error loading resource " + getSpaceId(), e);
    }
    return true;
  }

  @Override
  public void execute() throws Exception {
      System.out.println("EXECUTE");
      runReadQueryAsync(buildExportQuery(0), db(), 0);
      System.out.println("DONE");
  }

  @Override
  public void resume() throws Exception {

  }

  private SQLQuery buildTemporaryTableForExportQuery(String schema) {
    return new SQLQuery("""
                    CREATE TABLE IF NOT EXISTS ${schema}.${table}
                           (
                                s3_bucket text NOT NULL,
                                s3_path text NOT NULL,
                                s3_region text NOT NULL,
                                content_query text, --tileId/s3_path
                                state text NOT NULL, --jobtype
                                execution_count int DEFAULT 0, --amount of retries
                                data jsonb COMPRESSION lz4, --statistic data //getRowsUploaded	getFilesUploaded getBytesUploaded
                                i SERIAL,
                                CONSTRAINT ${primaryKey} PRIMARY KEY (s3_path)
                           );
                    """)
            .withVariable("table", TransportTools.getTemporaryJobTableName(this))
            .withVariable("schema", schema)
            .withVariable("primaryKey", TransportTools.getTemporaryJobTableName(this) + "_primKey");
  }

  private SQLQuery generateFilteredExportQuery(int threadNumber) throws WebClientException {
    calculatedThreadCount = 1;
    return new SQLQuery("${{exportQuery}} ${{threadCondition}}")
            .withQueryFragment("exportQuery" ,"Select * from ${schema}.${table}")
            .withQueryFragment("threadCondition"," WHERE i % " + calculatedThreadCount + " = " + threadNumber)
            .withVariable("table", getRootTableName(space))
            .withVariable("schema", getSchema(db()));
  }

  public SQLQuery buildExportQuery(int threadNumber) throws WebClientException {
    SQLQuery exportSelectString = generateFilteredExportQuery(threadNumber);

    String s3Path = outputS3Prefix(true,false) + "/" +UUID.randomUUID();
    s3Path += format.equals(Format.GEOJSON) ? ".geojson" : ".csv";

    return new SQLQuery(
            """
            PERFORM export_to_s3_perform(
                #{content_query}, #{s3_bucket}, #{s3_path}, #{s3_region}, 
                #{format}, #{filesize})
             """)
            .withNamedParameter("content_query", exportSelectString.substitute().text())
            .withNamedParameter("s3_bucket", bucketName())
            .withNamedParameter("s3_path", s3Path)
            .withNamedParameter("s3_region", bucketRegion())
            .withNamedParameter("format", format.name())
            .withNamedParameter("filesize", 0);
  }

  //TODO: De-duplicate once CSV was removed (see GeoJson format class)
  public enum EntityPerLine {
    Feature,
    FeatureCollection
  }
}
