/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.hub.rest;

import com.here.xyz.hub.config.DataReferenceConfigClient;
import com.here.xyz.models.hub.DataReference;
import com.here.xyz.util.service.HttpException;
import io.vertx.core.Future;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.here.xyz.hub.util.DataReferenceValidator.validateDataReference;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public final class DataReferenceApi extends Api {

  private final DataReferenceConfigClient dataReferences = DataReferenceConfigClient.getInstance();

  public DataReferenceApi(RouterBuilder rb) {
    rb.getRoute("createDataReference").setDoValidation(false).addHandler(handle(this::createDataReference, CREATED));
    rb.getRoute("getDataReference").setDoValidation(false).addHandler(handle(this::getDataReference));
    rb.getRoute("queryDataReferences").setDoValidation(false).addHandler(handle(this::queryDataReferences));
    rb.getRoute("deleteDataReference").setDoValidation(false).addHandler(handle(this::deleteDataReference));
  }

  private Future<DataReference> createDataReference(RoutingContext routingContext) {
    DataReference dataReference = dataReference(routingContext);

    return failIfDataReferenceIsInvalid(dataReference)
      .compose(this::failIfReferenceAlreadyExists)
      .compose(dataReferences::store)
      .map(dataReference::withId);
  }

  private static Future<DataReference> failIfDataReferenceIsInvalid(DataReference dataReference) {
    Collection<String> dataReferenceViolations = validateDataReference(dataReference);
    if (dataReferenceViolations.isEmpty()) {
      return Future.succeededFuture(dataReference);
    }

    return Future.failedFuture(
      new IllegalArgumentException("Invalid Data Reference:\n" + String.join("\n", dataReferenceViolations))
    );
  }

  private Future<DataReference> failIfReferenceAlreadyExists(DataReference dataReference) {
    return validateById(dataReference);
  }

  private Future<DataReference> validateById(DataReference dataReference) {
    return dataReference.getId() != null ? failIfPresentById(dataReference) : Future.succeededFuture(dataReference);
  }

  private Future<DataReference> failIfPresentById(DataReference dataReference) {
    return dataReferences
      .load(dataReference.getId())
      .compose(maybeReference ->
        maybeReference
          .<Future<DataReference>>map(reference ->
            Future.failedFuture(
              new IllegalArgumentException(
                "Overwriting existing Data Reference [id=%s] is not allowed.".formatted(reference.getId()))
              )
          )
          .orElse(Future.succeededFuture(dataReference))
      );
  }

  private static DataReference dataReference(RoutingContext routingContext) {
    return DatabindCodec.mapper().convertValue(routingContext.body().asJsonObject(), DataReference.class);
  }

  private Future<DataReference> getDataReference(RoutingContext routingContext) {
    UUID referenceId = referenceIdFromRequestPath(routingContext);
    return dataReferences.load(referenceId)
      .compose(maybeReference ->
        maybeReference.map(Future::succeededFuture)
          .orElse(Future.failedFuture(new HttpException(NOT_FOUND, "Data Reference id=%s not found".formatted(referenceId))))
      );
  }

  private Future<List<DataReference>> queryDataReferences(RoutingContext routingContext) {
    String entityId = extractAndValidateEntityId(routingContext);
    Integer startVersion = extractAndValidatePositiveInteger(
      routingContext,
      "startVersion",
      "DataReference's startVersion must be non-negative"
    );
    Integer endVersion = extractAndValidatePositiveInteger(
      routingContext,
      "endVersion",
      "DataReference's endVersion must be non-negative"
    );
    String contentType = firstQueryParamAsStringOrNull(routingContext, "contentType");
    String objectType = firstQueryParamAsStringOrNull(routingContext, "objectType");
    String sourceSystem = firstQueryParamAsStringOrNull(routingContext, "sourceSystem");
    String targetSystem = firstQueryParamAsStringOrNull(routingContext, "targetSystem");

    return dataReferences.load(entityId, startVersion, endVersion, contentType, objectType, sourceSystem, targetSystem);
  }

  private static @Nullable Integer extractAndValidatePositiveInteger(
    RoutingContext routingContext,
    String parameterName,
    String errorMessage
  ) {
    Integer extractedValue = firstQueryParamAsIntegerOrNull(routingContext, parameterName);
    checkArgument(extractedValue == null || extractedValue >= 0, errorMessage);

    return extractedValue;
  }

  private static String extractAndValidateEntityId(RoutingContext routingContext) {
    return firstQueryParamAsString(routingContext, "entityId")
      .orElseThrow(() -> new IllegalArgumentException("DataReference entityId must be present"));
  }

  private Future<Void> deleteDataReference(RoutingContext routingContext) {
    return dataReferences.delete(referenceIdFromRequestPath(routingContext));
  }

  private static UUID referenceIdFromRequestPath(RoutingContext routingContext) {
    String referenceId = routingContext.request().getParam("referenceId");
    checkArgument(isNotBlank(referenceId), "Data Reference ID cannot be blank");

    return UUID.fromString(referenceId);
  }

  private static String firstQueryParamAsStringOrNull(RoutingContext routingContext, String parameterName) {
    return firstQueryParamAsString(routingContext, parameterName)
      .orElse(null);
  }

  private static Optional<String> firstQueryParamAsString(RoutingContext routingContext, String parameterName) {
    return routingContext.queryParam(parameterName)
      .stream()
      .filter(StringUtils::isNotBlank)
      .findFirst();
  }

  private static Integer firstQueryParamAsIntegerOrNull(RoutingContext routingContext, String parameterName) {
    return firstQueryParamAsString(routingContext, parameterName)
      .map(Integer::valueOf)
      .orElse(null);
  }

}
