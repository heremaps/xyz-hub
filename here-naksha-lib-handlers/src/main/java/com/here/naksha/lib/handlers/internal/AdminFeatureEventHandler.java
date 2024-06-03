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
package com.here.naksha.lib.handlers.internal;

import static com.here.naksha.lib.handlers.AbstractEventHandler.EventProcessingStrategy.NOT_IMPLEMENTED;
import static com.here.naksha.lib.handlers.AbstractEventHandler.EventProcessingStrategy.PROCESS;
import static com.here.naksha.lib.handlers.internal.NakshaFeaturePropertiesValidator.nakshaFeatureValidation;

import com.here.naksha.lib.core.IEvent;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.naksha.NakshaFeature;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.handlers.AbstractEventHandler;
import com.here.naksha.lib.psql.PsqlStorage;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract event handler responsible for processing admin resources (like Storage or EventHandler)
 *
 * @param <FEATURE> type of admin resource handled by this handler
 */
abstract class AdminFeatureEventHandler<FEATURE extends NakshaFeature> extends AbstractEventHandler {

  private static final Logger logger = LoggerFactory.getLogger(AdminFeatureEventHandler.class);

  private final Class<FEATURE> featureClass;

  AdminFeatureEventHandler(@NotNull INaksha hub, @NotNull Class<FEATURE> featureClass) {
    super(hub);
    this.featureClass = featureClass;
  }

  @Override
  protected EventProcessingStrategy processingStrategyFor(IEvent event) {
    final Request<?> request = event.getRequest();
    if (request instanceof ReadRequest || request instanceof WriteXyzFeatures) {
      return PROCESS;
    }
    return NOT_IMPLEMENTED;
  }

  @Override
  public final @NotNull Result process(@NotNull IEvent event) {
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
          } else {
            logger.warn(
                "Failed writing feature request to admin storage, expected success but got: {}",
                result);
            writer.rollback(true);
          }
          return result;
        }
      }
    } else {
      return notImplemented(request);
    }
  }

  /**
   * Direct validation of XyzFeature to be written.
   *
   * @param codec containing the feature to be validated before being written
   * @return validation result
   */
  protected @NotNull Result validateFeature(XyzFeatureCodec codec) {
    final FEATURE feature = featureClass.cast(codec.getFeature());
    return nakshaFeatureValidation(feature);
  }

  private @NotNull Result validateWriteRequest(final @NotNull WriteXyzFeatures wr) {
    for (final XyzFeatureCodec featureCodec : wr.features) {
      Result featureValidation = validateFeature(featureCodec);
      if (featureValidation instanceof ErrorResult) {
        return featureValidation;
      }
    }
    return new SuccessResult();
  }
}
