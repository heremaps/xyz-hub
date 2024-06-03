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

import static com.here.naksha.lib.core.NakshaAdminCollection.EVENT_HANDLERS;
import static com.here.naksha.lib.core.util.storage.ResultHelper.readFeaturesFromResult;
import static com.here.naksha.lib.handlers.internal.PluginPropertiesValidator.pluginValidation;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.NoCursor;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.naksha.Storage;
import com.here.naksha.lib.core.models.storage.EWriteOp;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.PRef;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.SuccessResult;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodec;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import com.here.naksha.lib.handlers.DefaultStorageHandlerProperties;
import com.here.naksha.storage.http.HttpStorage;
import com.here.naksha.storage.http.HttpStorageProperties;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

public class IntHandlerForStorages extends AdminFeatureEventHandler<Storage> {

  private static final long MIN_HTTP_CONNECT_TIMEOUT_SEC = 0;
  private static final long MAX_HTTP_CONNECT_TIMEOUT_SEC = 30;

  private static final long MIN_HTTP_SOCKET_TIMEOUT_SEC = 0;
  private static final long MAX_HTTP_SOCKET_TIMEOUT_SEC = 90;

  private static final Set<String> ALLOWED_PROTOCOLS = Set.of("http", "https");

  public IntHandlerForStorages(final @NotNull INaksha hub) {
    super(hub, Storage.class);
  }

  @Override
  protected @NotNull Result validateFeature(XyzFeatureCodec codec) {
    final EWriteOp operation = EWriteOp.get(codec.getOp());
    if (operation.equals(EWriteOp.DELETE)) {
      // For DELETE, only the feature ID is needed, other JSON properties are irrelevant
      return noActiveHandlerValidation(codec);
    }
    // For non-DELETE write request
    Result basicValidation = super.validateFeature(codec);
    if (basicValidation instanceof ErrorResult) {
      return basicValidation;
    }
    final Storage storage = (Storage) codec.getFeature();
    Result pluginValidation = pluginValidation(storage);
    if (pluginValidation instanceof ErrorResult) {
      return pluginValidation;
    }
    return httpStorageValidation(storage);
  }

  private Result httpStorageValidation(Storage storage) {
    if (HttpStorage.class.getName().equals(storage.getClassName())) {
      HttpStorageProperties httpStorageProperties = null;
      try {
        httpStorageProperties = JsonSerializable.convert(storage.getProperties(), HttpStorageProperties.class);
      } catch (Exception e) {
        return new ErrorResult(
            XyzError.ILLEGAL_ARGUMENT,
            "Unable to convert 'properties' to " + HttpStorageProperties.class.getName(),
            e);
      }
      return httpStoragePropertiesValidation(httpStorageProperties);
    }
    return new SuccessResult();
  }

  private Result httpStoragePropertiesValidation(HttpStorageProperties httpStorageProperties) {
    boolean isConnectionTimeoutValid = isBetween(
        httpStorageProperties.getConnectTimeout(), MIN_HTTP_CONNECT_TIMEOUT_SEC, MAX_HTTP_CONNECT_TIMEOUT_SEC);
    boolean isSocketTimeoutValid = isBetween(
        httpStorageProperties.getSocketTimeout(), MIN_HTTP_SOCKET_TIMEOUT_SEC, MAX_HTTP_SOCKET_TIMEOUT_SEC);
    boolean isUrlValid = isUrlValid(httpStorageProperties.getUrl());
    if (isConnectionTimeoutValid && isSocketTimeoutValid && isUrlValid) {
      return new SuccessResult();
    }
    String errorMsg =
        getErrorMsg(httpStorageProperties, isConnectionTimeoutValid, isSocketTimeoutValid, isUrlValid);
    return new ErrorResult(XyzError.ILLEGAL_ARGUMENT, errorMsg);
  }

  @NotNull
  private static String getErrorMsg(
      HttpStorageProperties httpStorageProperties,
      boolean isConnectionTimeoutValid,
      boolean isSocketTimeoutValid,
      boolean isUrlValid) {
    ArrayList<String> errorMsgs = new ArrayList<>(3);
    if (!isConnectionTimeoutValid) {
      errorMsgs.add("Invalid connection timeout: %d, allowed values (sec): %d - %d"
          .formatted(
              httpStorageProperties.getConnectTimeout(),
              MIN_HTTP_CONNECT_TIMEOUT_SEC,
              MAX_HTTP_CONNECT_TIMEOUT_SEC));
    }
    if (!isSocketTimeoutValid) {
      errorMsgs.add("Invalid socket timeout: %d, allowed values (sec): %d - %d"
          .formatted(
              httpStorageProperties.getSocketTimeout(),
              MIN_HTTP_SOCKET_TIMEOUT_SEC,
              MAX_HTTP_SOCKET_TIMEOUT_SEC));
    }
    if (!isUrlValid) {
      errorMsgs.add("Invalid url: %s".formatted(httpStorageProperties.getUrl()));
    }
    return String.join("\n", errorMsgs);
  }

  private boolean isBetween(long value, long min, long max) {
    return value >= min && value <= max;
  }

  private boolean isUrlValid(String maybeUrl) {
    try {
      URL url = new URL(maybeUrl);
      return ALLOWED_PROTOCOLS.contains(url.getProtocol());
    } catch (MalformedURLException e) {
      return false;
    }
  }

  private Result noActiveHandlerValidation(XyzFeatureCodec codec) {
    // Search for active event handlers still using this storage
    String storageId = codec.getId();
    if (storageId == null) {
      if (codec.getFeature() == null) {
        return new ErrorResult(XyzError.ILLEGAL_ARGUMENT, "No storage ID supplied.");
      }
      storageId = codec.getFeature().getId();
    }
    // Scan through all handlers with JSON property "properties.storageId" = <storage-id-to-be-deleted>
    final PRef pRef = RequestHelper.pRefFromPropPath(
        new String[] {XyzFeature.PROPERTIES, DefaultStorageHandlerProperties.STORAGE_ID});
    final POp activeHandlersPOp = POp.eq(pRef, storageId);
    final ReadFeatures readActiveHandlersRequest =
        new ReadFeatures(EVENT_HANDLERS).withPropertyOp(activeHandlersPOp);
    try (final IReadSession readSession =
        nakshaHub().getAdminStorage().newReadSession(NakshaContext.currentContext(), false)) {
      final Result readResult = readSession.execute(readActiveHandlersRequest);
      if (!(readResult instanceof SuccessResult)) {
        return readResult;
      }
      final List<EventHandler> eventHandlers;
      try {
        eventHandlers = readFeaturesFromResult(readResult, EventHandler.class);
      } catch (NoCursor | NoSuchElementException emptyException) {
        // No active handler using the storage, proceed with deleting the storage
        return new SuccessResult();
      } finally {
        readResult.close();
      }
      final List<String> handlerIds =
          eventHandlers.stream().map(XyzFeature::getId).toList();
      return new ErrorResult(
          XyzError.CONFLICT, "The storage is still in use by these event handlers: " + handlerIds);
    }
  }
}
