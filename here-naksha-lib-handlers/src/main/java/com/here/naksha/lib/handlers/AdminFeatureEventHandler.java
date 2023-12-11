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
package com.here.naksha.lib.handlers;

import com.here.naksha.lib.core.IEvent;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.ReadRequest;
import com.here.naksha.lib.core.models.storage.Request;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.SuccessResult;
import com.here.naksha.lib.core.models.storage.WriteXyzFeatures;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodec;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.psql.PsqlStorage;
import org.jetbrains.annotations.NotNull;

/**
 * Abstract event handler responsible for processing admin resources (like Storage or EventHandler)
 *
 * @param <FEATURE> type of admin resource handled by this handler
 */
abstract class AdminFeatureEventHandler<FEATURE extends XyzFeature> extends AbstractEventHandler {

  private final Class<FEATURE> featureClass;

  AdminFeatureEventHandler(@NotNull INaksha hub, @NotNull Class<FEATURE> featureClass) {
    super(hub);
    this.featureClass = featureClass;
  }

  /**
   * The method invoked by the event-pipeline to process EventHandler specific read/write operations
   *
   * @param event the event to process.
   * @return the result.
   */
  @Override
  public final @NotNull Result processEvent(@NotNull IEvent event) {
    final NakshaContext ctx = NakshaContext.currentContext();
    final Request<?> request = event.getRequest();
    // process request using Naksha Admin Storage instance
    addStorageIdToStreamInfo(PsqlStorage.ADMIN_STORAGE_ID, ctx);
    if (request instanceof ReadRequest<?> rr) {
      try (final IReadSession reader = nakshaHub().getAdminStorage().newReadSession(ctx, false)) {
        return reader.execute(rr);
      }
    } else if (request instanceof WriteXyzFeatures wr) {
      // validate the request before persisting
      try (Result valResult = validateWriteRequest(wr)) {
        if (valResult instanceof ErrorResult er) {
          return er;
        }
        // persist in storage
        try (final IWriteSession writer = nakshaHub().getAdminStorage().newWriteSession(ctx, true)) {
          final Result result = writer.execute(wr);
          if (result instanceof SuccessResult) {
            writer.commit(true);
          }
          return result;
        }
      }
    } else {
      return notImplemented(event);
    }
  }

  /**
   * Direct validation of XyzFeature to be written. It's optional to implement this, by default it always succeeds.
   *
   * @param feature feature to be validated before being written
   * @return validation result, success by default
   */
  protected @NotNull Result validateFeature(FEATURE feature) {
    return new SuccessResult();
  }

  private @NotNull Result validateWriteRequest(final @NotNull WriteXyzFeatures wr) {
    for (final XyzFeatureCodec featureCodec : wr.features) {
      final FEATURE feature = featureClass.cast(featureCodec.getFeature());
      Result featureValidation = validateFeature(feature);
      if (featureValidation instanceof ErrorResult) {
        return featureValidation;
      }
    }
    return new SuccessResult();
  }
}
