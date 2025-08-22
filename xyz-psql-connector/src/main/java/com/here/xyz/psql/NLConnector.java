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

package com.here.xyz.psql;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.DeleteChangesetsEvent;
import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySubscriptionEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.query.GetFastStatistics;
import com.here.xyz.psql.query.GetFeaturesByBBox;
import com.here.xyz.psql.query.GetFeaturesByGeometry;
import com.here.xyz.psql.query.GetFeaturesById;
import com.here.xyz.psql.query.GetStatistics;
import com.here.xyz.psql.query.ModifySpace;
import com.here.xyz.responses.BinaryResponse;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.responses.changesets.ChangesetCollection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.here.xyz.util.db.ConnectorParameters.TableLayout.V2;

import static com.here.xyz.responses.XyzError.NOT_IMPLEMENTED;

public class NLConnector extends PSQLXyzConnector {
  private static final Logger logger = LogManager.getLogger();

  @Override
  protected StatisticsResponse processGetStatistics(GetStatisticsEvent event) throws Exception {
    if(event.isFastMode())
      return run(new GetFastStatistics(event).withTableLayout(V2));
    return run(new GetStatistics(event).withTableLayout(V2));
  }

  @Override
  protected FeatureCollection processGetFeaturesByIdEvent(GetFeaturesByIdEvent event) throws Exception {
    return run(new GetFeaturesById(event).withTableLayout(V2));
  }

  @Override
  protected FeatureCollection processGetFeaturesByGeometryEvent(GetFeaturesByGeometryEvent event) throws Exception {
    return run(new GetFeaturesByGeometry(event).withTableLayout(V2));
  }

  @Override
  protected FeatureCollection processGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception {
    checkForInvalidHereTileClustering(event);
    return run(getBBoxBasedQueryRunner(event));
  }

  @Override
  protected BinaryResponse processBinaryGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception {
    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
  }

  @Override
  protected FeatureCollection processGetFeaturesByBBoxEvent(GetFeaturesByBBoxEvent event) throws Exception {
    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
  }

  private <R extends XyzResponse> GetFeaturesByBBox<GetFeaturesByBBoxEvent, R> getBBoxBasedQueryRunner(GetFeaturesByBBoxEvent event)
      throws Exception {
    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
  }

  @Override
  protected FeatureCollection processIterateFeaturesEvent(IterateFeaturesEvent event) throws Exception {
    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
  }

  @Override
  protected FeatureCollection processSearchForFeaturesEvent(SearchForFeaturesEvent event) throws Exception {
    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
  }

  @Override
  protected FeatureCollection processLoadFeaturesEvent(LoadFeaturesEvent event) throws Exception {
    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
  }

  @Override
  protected FeatureCollection processModifyFeaturesEvent(ModifyFeaturesEvent event) throws Exception {
    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
  }

  @Override
  protected FeatureCollection processWriteFeaturesEvent(WriteFeaturesEvent event) throws Exception {
    //TODO: only support this write event
    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
  }

  @Override
  protected SuccessResponse processModifySpaceEvent(ModifySpaceEvent event) throws Exception {
    return write(new ModifySpace(event).withTableLayout(V2));
  }

  @Deprecated
  @Override
  protected SuccessResponse processModifySubscriptionEvent(ModifySubscriptionEvent event) throws Exception {
    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
  }

//  @Override
//  protected StorageStatistics processGetStorageStatisticsEvent(GetStorageStatisticsEvent event) throws Exception {
//    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
//  }

  @Override
  protected SuccessResponse processDeleteChangesetsEvent(DeleteChangesetsEvent event) throws Exception {
    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
  }

  @Override
  protected ChangesetCollection processIterateChangesetsEvent(IterateChangesetsEvent event) throws Exception {
    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
  }

  @Override
  protected ChangesetsStatisticsResponse processGetChangesetsStatisticsEvent(GetChangesetStatisticsEvent event) throws Exception {
    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
  }
}
