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
package com.here.naksha.app.data;

import static com.here.naksha.app.common.TestUtil.parseJsonFileOrFail;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.app.service.models.FeatureCollectionRequest;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDataIngest {
  private static final Logger logger = LoggerFactory.getLogger(AbstractDataIngest.class);

  protected static final String DATA_ROOT_FOLDER = "src/test/resources/ingest_data/";
  protected static final String DEF_BATCH_SIZE = "100";

  protected String nhUrl = "http://localhost:8080/";
  protected String nhToken = "";
  protected String nhSpaceId = "no-space";
  protected int reqBatchSize = Integer.parseInt(DEF_BATCH_SIZE);
  protected NakshaTestWebClient nakshaClient;

  protected void setNHUrl(final @NotNull String nhUrl) {
    this.nhUrl = nhUrl;
  }

  protected void setNHToken(final @NotNull String nhToken) {
    this.nhToken = nhToken;
  }

  protected void setNHSpaceId(final @NotNull String nhSpaceId) {
    this.nhSpaceId = nhSpaceId;
  }

  protected void setReqBatchSize(final int reqBatchSize) {
    this.reqBatchSize = reqBatchSize;
  }

  protected void setNakshaClient(final @NotNull NakshaTestWebClient nakshaClient) {
    this.nakshaClient = nakshaClient;
  }

  protected void ingestData(final @NotNull String filePath) throws Exception {

    // Prepare REST API request
    final FeatureCollectionRequest collectionRequest =
        parseJsonFileOrFail(DATA_ROOT_FOLDER, filePath, FeatureCollectionRequest.class);
    final List<XyzFeature> features = (List<XyzFeature>) collectionRequest.getFeatures();
    nullifyUuid(features);
    final String streamId = UUID.randomUUID().toString();

    // Prepare and submit as a Batch request
    final int totalFeatures = features.size();
    int crtIdx = 0;
    int batchCnt = 0;
    while (crtIdx < totalFeatures) {
      final String batchRequest = prepareNextBatchRequest(features, totalFeatures, crtIdx, reqBatchSize);
      batchCnt++;

      // Submit Upsert Features request to NakshaHub URL
      final HttpResponse<String> response = nakshaClient.put(
          "hub/spaces/" + nhSpaceId + "/features?access_token=" + nhToken, batchRequest, streamId);

      // Perform assertion
      assertEquals(
          200,
          response.statusCode(),
          "ResCode mismatch while importing batch " + batchCnt + ", streamId was " + streamId);

      // go for next batch (if available)
      crtIdx += reqBatchSize;
      logger.info("Import of batch [{}], with max size of [{}] is successful!", batchCnt, reqBatchSize);
    }

    logger.info("Import of all [{}] features successful!", totalFeatures);
  }

  private String prepareNextBatchRequest(
      final @NotNull List<XyzFeature> features,
      final int totalFeatures,
      final int crtIdx,
      final int reqBatchSize) {
    final FeatureCollectionRequest request = new FeatureCollectionRequest();
    int endIdx = Math.min(crtIdx + reqBatchSize, totalFeatures);
    return request.withFeatures(features.subList(crtIdx, endIdx)).serialize();
  }

  private void nullifyUuid(final @NotNull List<XyzFeature> features) {
    for (final XyzFeature feature : features) {
      feature.getProperties().getXyzNamespace().setUuid(null);
    }
  }
}
