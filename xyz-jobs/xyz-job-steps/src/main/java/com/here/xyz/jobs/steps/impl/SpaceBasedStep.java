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

package com.here.xyz.jobs.steps.impl;

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.getTableNameFromSpaceParamsOrSpaceId;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.execution.db.DatabaseBasedStep;
import com.here.xyz.jobs.steps.impl.transport.CopySpace;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import com.here.xyz.jobs.util.S3Client;
import com.here.xyz.models.geojson.coordinates.JTSHelper;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.ConnectorParameters;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

@JsonSubTypes({
    @JsonSubTypes.Type(value = CreateIndex.class),
    @JsonSubTypes.Type(value = ImportFilesToSpace.class),
    @JsonSubTypes.Type(value = DropIndexes.class),
    @JsonSubTypes.Type(value = AnalyzeSpaceTable.class),
    @JsonSubTypes.Type(value = MarkForMaintenance.class),
    @JsonSubTypes.Type(value = CopySpace.class)
})
public abstract class SpaceBasedStep<T extends SpaceBasedStep> extends DatabaseBasedStep<T> {
  private static final Logger logger = LogManager.getLogger();

  @JsonView({Internal.class, Static.class})
  private String spaceId;

  public String getSpaceId() {
    return spaceId;
  }

  public void setSpaceId(String spaceId) {
    this.spaceId = spaceId;
  }

  public T withSpaceId(String spaceId) {
    setSpaceId(spaceId);
    return (T) this;
  }

  protected final String getRootTableName(String spaceId) throws WebClientException {
    return getRootTableName(loadSpace(spaceId));
  }

  protected final String getRootTableName(Space space) throws WebClientException {
    return getTableNameFromSpaceParamsOrSpaceId(space.getStorage().getParams(), space.getId(),
        ConnectorParameters.fromMap(hubWebClient().loadConnector(space.getStorage().getId()).params).isEnableHashedSpaceId());
  }

  protected final boolean isEnableHashedSpaceIdActivated(Space space) throws WebClientException {
    return ConnectorParameters.fromMap(hubWebClient().loadConnector(space.getStorage().getId()).params).isEnableHashedSpaceId();
  }

  protected void validateSpaceExists() throws ValidationException {
    try {
      //Check if the space is actually existing
      if (getSpaceId() == null)
        throw new ValidationException("SpaceId is missing!");
      loadSpace(getSpaceId());
    }
    catch (WebClientException e) {
      throw new ValidationException("Error loading resource " + getSpaceId(), e);
    }
  }

  protected Space loadSpace(String spaceId) throws WebClientException {
    return hubWebClient().loadSpace(spaceId);
  }

  protected StatisticsResponse loadSpaceStatistics(String spaceId, SpaceContext context) throws WebClientException {
    return hubWebClient().loadSpaceStatistics(spaceId, context);
  }

  protected Tag loadTag(String spaceId, String tagId) throws WebClientException {
    return hubWebClient().loadTag(spaceId, tagId);
  }

  protected HubWebClient hubWebClient() {
    return HubWebClient.getInstance(Config.instance.HUB_ENDPOINT);
  }

  protected void streamFileToSpace(String s3Path, ImportFilesToSpace.Format format, boolean isGzipped)
          throws IOException, InterruptedException, WebClientException, ParseException {
    int retryDelayMs = 5_000;
    int maxRetries = 5;

    final S3Client s3Client = S3Client.getInstance();
    long MAX_READ_SIZE = 5 * 1024 * 1024 ;

    InputStream decompressedStream = isGzipped ? new GZIPInputStream(s3Client.streamObjectContent(s3Path))
            : s3Client.streamObjectContent(s3Path);

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(decompressedStream, StandardCharsets.UTF_8))) {
      StringBuilder dataBuffer = new StringBuilder();
      long bytesRead = 0;

      String line;
      while ((line = reader.readLine()) != null) {
        dataBuffer.append(line).append(format.equals(ImportFilesToSpace.Format.GEOJSON) ? "," : "##");
        bytesRead += line.getBytes().length;

        if (bytesRead >= MAX_READ_SIZE) {
          putFeaturesWithRetry(createFeatureCollection(dataBuffer.toString(), format), maxRetries, retryDelayMs, bytesRead);
          bytesRead = 0;
          dataBuffer = new StringBuilder();
        }
      }

      // Send remaining data if any
      if (dataBuffer.length() > 0) {
        putFeaturesWithRetry(createFeatureCollection(dataBuffer.toString(), format), maxRetries, retryDelayMs, bytesRead);
      }
    }finally {
      decompressedStream.close();
    }
  }

  private FeatureCollection createFeatureCollection(String featureList, ImportFilesToSpace.Format format)
          throws JsonProcessingException, ParseException {
    if(format.equals(ImportFilesToSpace.Format.GEOJSON)) {

    }else if(format.equals(ImportFilesToSpace.Format.CSV_GEOJSON)){
      String[] split = featureList.split("##");

      StringBuilder buffer = new StringBuilder();
      for (String csvGeojson : split) {
        buffer.append(csvGeojson.substring(1, csvGeojson.length() -1).replaceAll("'\"", "\"")).append(",");
      }
      featureList = buffer.toString();
      buffer = null;
    }else if(format.equals(ImportFilesToSpace.Format.CSV_JSON_WKB)){
      String[] split = featureList.split("##");
      String result = "";

      for (String csvGeojson : split) {
        JSONObject json = new JSONObject(csvGeojson.substring(1, csvGeojson.lastIndexOf(",") - 1).replaceAll("'\"", "\""));
        json.put("type","Feature");
        Feature feature = XyzSerializable.deserialize(json.toString());

        String wkb = csvGeojson.substring(csvGeojson.lastIndexOf(",") + 1);
        byte[] aux = WKBReader.hexToBytes(wkb);
        feature.setGeometry(JTSHelper.fromGeometry(new WKBReader().read(aux)));
        result += XyzSerializable.serialize(feature) + ",";
      }
      featureList = result;
    }
    //In case Format=GEOJSON nothing additional to do!
    //cut last comma
    featureList = featureList.substring(0, featureList.length() - 1);
    return XyzSerializable.deserialize("{\"type\": \"FeatureCollection\",\"features\": [" + featureList + "]}");
  }

  protected void putFeaturesWithRetry(FeatureCollection fc, int maxRetries, int delayMs, long bytes)
          throws WebClientException, InterruptedException {
    try {
      logger.info("[{}] Uploading {} features-bytes to service.", getGlobalStepId(), bytes);
      hubWebClient().putFeaturesWithoutResponse(spaceId, fc);
    } catch (WebClientException e) {
      if(e instanceof XyzWebClient.ErrorResponseException){
        //code>=400
        switch (((XyzWebClient.ErrorResponseException) e).getErrorResponse().statusCode()){
          case 429 :
          case 504 :
            if(maxRetries != 0) {
              logger.info("[{}] Retry upload feature request! bytes: {}, maxRetries {} ", getGlobalStepId(), bytes, maxRetries, e );
              TimeUnit.MILLISECONDS.sleep(delayMs);
              putFeaturesWithRetry(fc, --maxRetries, delayMs, bytes);
            }else
              throw e;
            break;
          default:
            logger.warn("[{}] Error uploading features to service! ", getGlobalStepId(), e);
            throw e;
        }
      }
      else{
        logger.warn("Error uploading features to service! ",e);
        throw e;
      }
    }
  }

  @Override
  public boolean validate() throws ValidationException {
    validateSpaceExists();
    //Return true as no user inputs are needed
    return true;
  }
}
