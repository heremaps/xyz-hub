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
import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.jobs.steps.execution.db.Database;
import com.here.xyz.jobs.steps.impl.SpaceBasedStep;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.List;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole.WRITER;
import static com.here.xyz.jobs.steps.execution.db.Database.loadDatabase;
import static com.here.xyz.util.web.XyzWebClient.WebClientException;

/**
 * This step imports a set of user provided inputs and imports their data into a specified space.
 * This step produces exactly one output of type {@link FeatureStatistics}.
 */
public class ExportSpaceToFiles extends SpaceBasedStep<ExportSpaceToFiles> {
  private static final Logger logger = LogManager.getLogger();

  private Format format = Format.GEOJSON;
  private Phase phase;

  //Geometry-Filters
  private Geometry geometry;
  private int radius = -1;
  private boolean clipOnFilterGeometry;

  //Content-Filters
  private String propertyFilter;
  private ContextAwareEvent.SpaceContext context;
  private String targetVersion;

  //Partitioning
  private String partitionKey;
  //Required if partitionKey=tileId
  private Integer targetLevel;
  private boolean clipOnPartitions;

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

  public Phase getPhase() {
    return phase;
  }

  @JsonView({Internal.class, Static.class})
  private StatisticsResponse statistics = null;

  @Override
  public List<Load> getNeededResources() {
    return List.of();
  }

  @Override
  public int getTimeoutSeconds() {
    return 0;
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
    logger.info("EXECUTE");
    logger.info( "Loading space config for space "+getSpaceId());
    Space space = loadSpace(getSpaceId());
    logger.info("Getting storage database for space  "+getSpaceId());
    Database db = loadDatabase(space.getStorage().getId(), WRITER);
  }

  @Override
  public void resume() throws Exception {

  }

  private SQLQuery buildTemporaryTableForImportQuery(String schema) {
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
            .withVariable("table", TransportTools.getTemporaryTableName(this))
            .withVariable("schema", schema)
            .withVariable("primaryKey", TransportTools.getTemporaryTableName(this) + "_primKey");
  }

  private SQLQuery generateFilteredExportQuery(
              SQLQuery customWhereCondition,
              boolean isForCompositeContentDetection,
              String partitionKey,
              Boolean omitOnNull )
          throws SQLException {

      return null;
  }

  public SQLQuery buildS3ExportQuery(String s3Bucket, String s3Path, String s3FilePrefix, String s3Region) {
    s3Path = s3Path+ "/" +(s3FilePrefix == null ? "" : s3FilePrefix)+"export";

    SQLQuery exportSelectString = new SQLQuery("");
    String exportOptions = "";

    if(format.equals(Format.GEOJSON)){
      exportOptions = " 'FORMAT TEXT, ENCODING ''UTF8'' '";
      s3Path += ".geojson";
    }else {
      exportOptions = "'format csv,delimiter '','', encoding ''UTF8'', quote  ''\"'', escape '''''''' '";
      s3Path += ".csv";
    }

    SQLQuery q = new SQLQuery("SELECT * from aws_s3.query_export_to_s3("+
            " ${{exportSelectString}},"+
            " aws_commons.create_s3_uri(#{s3Bucket}, #{s3Path}, #{s3Region}),"+
            " options := "+exportOptions+");"
    );

    q.setQueryFragment("exportSelectString", exportSelectString);
    q.setNamedParameter("s3Bucket",s3Bucket);
    q.setNamedParameter("s3Path",s3Path);
    q.setNamedParameter("s3Region",s3Region);

    return q;
  }
}
