/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.hub.connectors;

import static com.here.xyz.responses.XyzError.NOT_FOUND;
import static com.here.xyz.util.Tools.parallelize;

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
import com.here.xyz.events.ModifyBranchEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySubscriptionEvent;
import com.here.xyz.events.PutBlobTileEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.hub.Config;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.BinaryResponse;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
import com.here.xyz.responses.ModifiedBranchResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.StatisticsResponse.Value;
import com.here.xyz.responses.StorageStatistics;
import com.here.xyz.responses.StorageStatistics.SpaceByteSizes;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.responses.changesets.ChangesetCollection;
import com.here.xyz.util.service.aws.s3.S3Client;
import com.here.xyz.util.service.aws.s3.S3Client.S3ObjectWithMetadata;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

public class BlobConnector extends StorageConnector {

  private S3Client s3() {
    return S3Client.getInstance(Config.instance.XYZ_HUB_S3_BUCKET);
  }

  private String s3Key(String spaceId, String tileId) {
    return "binarySpaces/" + spaceId + "/" + tileId;
  }

  @Override
  protected BinaryResponse processBinaryGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception {
    //TODO: Return author as part of response?
    try {
      S3ObjectWithMetadata s3ObjectData = s3().loadObjectContentWithMetadata(s3Key(event.getSpace(), event.getQuadkey()));
      return new BinaryResponse()
          .withBytes(s3ObjectData.content())
          .withMimeType(s3ObjectData.contentType());
    }
    catch (NoSuchKeyException e) {
      throw new ErrorResponseException(NOT_FOUND, "No data exists for the specified tile ID.");
    }
  }

  @Override
  protected SuccessResponse processPutBlobTileEvent(PutBlobTileEvent event) throws Exception {
    s3().putObject(s3Key(event.getSpace(), event.getTileId()), event.getMimeType(), event.getBytes(), true, Map.of("Author",
        event.getAuthor()));
    return new SuccessResponse().withStatus("OK");
  }

  @Override
  protected SuccessResponse processModifySpaceEvent(ModifySpaceEvent event) throws Exception {
    return new SuccessResponse().withStatus("OK");
  }

  @Override
  protected StorageStatistics processGetStorageStatisticsEvent(GetStorageStatisticsEvent event) throws Exception {
    return parallelize(() -> {
      Map<String, SpaceByteSizes> byteSizes = event.getSpaceIds().parallelStream()
          .collect(Collectors.toMap(Function.identity(),
              spaceId -> new SpaceByteSizes().withContentBytes(new Value<>(aggregateContentBytes(spaceId)))));

      return new StorageStatistics()
          .withByteSizes(byteSizes)
          .withCreatedAt(System.currentTimeMillis());
    }, 20);
  }


  private long aggregateContentBytes(String spaceId) {
    return s3().scanFolder(s3Key(spaceId, "")).stream()
        .mapToLong(s3ObjectSummary -> s3ObjectSummary.size()).sum();
  }

  @Override
  protected StatisticsResponse processGetStatistics(GetStatisticsEvent event) throws Exception {
    return notImplemented(event);
  }

  @Override
  protected FeatureCollection processGetFeaturesByIdEvent(GetFeaturesByIdEvent event) throws Exception {
    return notImplemented(event);
  }

  @Override
  protected FeatureCollection processGetFeaturesByGeometryEvent(GetFeaturesByGeometryEvent event) throws Exception {
    return notImplemented(event);
  }

  @Override
  protected FeatureCollection processGetFeaturesByBBoxEvent(GetFeaturesByBBoxEvent event) throws Exception {
    return notImplemented(event);
  }

  @Override
  protected FeatureCollection processGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception {
    return notImplemented(event);
  }

  @Override
  protected FeatureCollection processIterateFeaturesEvent(IterateFeaturesEvent event) throws Exception {
    return notImplemented(event);
  }

  @Override
  protected FeatureCollection processSearchForFeaturesEvent(SearchForFeaturesEvent event) throws Exception {
    return notImplemented(event);
  }

  @Override
  protected FeatureCollection processLoadFeaturesEvent(LoadFeaturesEvent event) throws Exception {
    return notImplemented(event);
  }

  @Override
  protected FeatureCollection processModifyFeaturesEvent(ModifyFeaturesEvent event) throws Exception {
    return notImplemented(event);
  }

  @Override
  protected FeatureCollection processWriteFeaturesEvent(WriteFeaturesEvent event) throws Exception {
    return notImplemented(event);
  }

  @Override
  protected SuccessResponse processModifySubscriptionEvent(ModifySubscriptionEvent event) throws Exception {
    return notImplemented(event);
  }

  @Override
  protected SuccessResponse processDeleteChangesetsEvent(DeleteChangesetsEvent event) throws Exception {
    return notImplemented(event);
  }

  @Override
  protected ChangesetCollection processIterateChangesetsEvent(IterateChangesetsEvent event) throws Exception {
    return notImplemented(event);
  }

  @Override
  protected ChangesetsStatisticsResponse processGetChangesetsStatisticsEvent(GetChangesetStatisticsEvent event) throws Exception {
    return notImplemented(event);
  }

  @Override
  protected ModifiedBranchResponse processModifyBranchEvent(ModifyBranchEvent event) throws ErrorResponseException {
    return notImplemented(event);
  }

  private static <E extends XyzResponse> E notImplemented(Event event) {
    throw new UnsupportedOperationException(event.getClass().getSimpleName() + " not implemented.");
  }

  @Override
  protected void handleProcessingException(Exception exception, Event event) throws Exception {
    throw exception;
  }

  @Override
  protected void initialize(Event event) throws Exception {
    //Nothing to do here
  }
}
