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
package com.here.naksha.lib.handlers.util;

import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.EChangeState;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.EReviewState;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.HereDeltaNs;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.models.storage.*;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class HandlerUtil {

  public static String REVIEW_STATE_PREFIX = "@:review-state:";

  private HandlerUtil() {}

  public static @NotNull ContextXyzFeatureResult createContextResultFromFeatureList(
      final @NotNull List<XyzFeature> features,
      final @Nullable List<XyzFeature> context,
      final @Nullable List<XyzFeature> violations) {
    // Create ForwardCursor with input features
    final List<XyzFeatureCodec> codecs = new ArrayList<>();
    final XyzFeatureCodecFactory codecFactory = XyzFeatureCodecFactory.get();
    for (final XyzFeature feature : features) {
      final XyzFeatureCodec codec = codecFactory.newInstance();
      codec.setOp(EExecutedOp.UPDATED);
      codec.setFeature(feature);
      codecs.add(codec);
    }
    final ListBasedForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        new ListBasedForwardCursor<>(codecFactory, codecs);

    // Create ContextResult with cursor, context and violations
    final ContextXyzFeatureResult ctxResult = new ContextXyzFeatureResult(cursor);
    ctxResult.setContext(context);
    ctxResult.setViolations(violations);
    return ctxResult;
  }

  public static @NotNull ContextWriteXyzFeatures createContextWriteRequestFromFeatureList(
      final @NotNull String collectionId,
      final @NotNull List<?> features,
      final @Nullable List<?> context,
      final @Nullable List<?> violations) {
    // generate new ContextWriteFeatures request
    final ContextWriteXyzFeatures cwf = new ContextWriteXyzFeatures(collectionId);

    // Add features in the request
    for (final Object obj : features) {
      final XyzFeature feature = checkInstanceOf(obj, XyzFeature.class, "Unsupported feature type");
      cwf.add(EWriteOp.PUT, feature);
    }
    // add context to write request
    cwf.setContext(getXyzContextFromGenericList(context));
    // add violations to write request
    cwf.setViolations(getXyzViolationsFromGenericList(violations));
    return cwf;
  }

  public static @NotNull ContextWriteXyzFeatures createContextWriteRequestFromCodecList(
      final @NotNull String collectionId,
      final @NotNull List<?> inputCodecs,
      final @Nullable List<?> context,
      final @Nullable List<?> violations) {
    // generate new ContextWriteFeatures request
    final ContextWriteXyzFeatures cwf = new ContextWriteXyzFeatures(collectionId);

    // Add features in the request
    if (inputCodecs.isEmpty()) throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "No features supplied");
    for (final Object inputCodec : inputCodecs) {
      final XyzFeatureCodec xyzCodec =
          checkInstanceOf(inputCodec, XyzFeatureCodec.class, "Unsupported feature codec type");
      final XyzFeature feature =
          HandlerUtil.checkInstanceOf(xyzCodec.getFeature(), XyzFeature.class, "Unsupported feature type");
      cwf.add(EWriteOp.get(xyzCodec.getOp()), feature);
    }

    // add context to write request
    cwf.setContext(getXyzContextFromGenericList(context));

    // add violations to write request
    cwf.setViolations(getXyzViolationsFromGenericList(violations));

    return cwf;
  }

  public static @NotNull List<XyzFeature> getXyzFeaturesFromCodecList(final @NotNull List<?> codecs) {
    final List<XyzFeature> outputFeatures = new ArrayList<>();
    for (final Object obj : codecs) {
      final XyzFeatureCodec codec = checkInstanceOf(obj, XyzFeatureCodec.class, "Unsupported feature codec");
      outputFeatures.add(codec.getFeature());
    }
    return outputFeatures;
  }

  public static @Nullable List<XyzFeature> getXyzViolationsFromGenericList(final @Nullable List<?> violations) {
    List<XyzFeature> outputViolations = null;
    if (violations != null) {
      for (final Object obj : violations) {
        final XyzFeature violation = checkInstanceOf(
            obj, XyzFeature.class, XyzError.EXCEPTION, "Unsupported violation feature type");
        if (outputViolations == null) outputViolations = new ArrayList<>();
        // Add violation to output list
        outputViolations.add(violation);
      }
    }
    return outputViolations;
  }

  public static @Nullable List<XyzFeature> getXyzContextFromGenericList(final @Nullable List<?> contextList) {
    List<XyzFeature> outputCtx = null;
    if (contextList != null) {
      for (final Object obj : contextList) {
        final XyzFeature context =
            checkInstanceOf(obj, XyzFeature.class, XyzError.EXCEPTION, "Unsupported context feature type");
        if (outputCtx == null) outputCtx = new ArrayList<>();
        // Add context to output list
        outputCtx.add(context);
      }
    }
    return outputCtx;
  }

  private static @NotNull List<String> tagsWithoutReviewState(@Nullable List<String> tags) {
    if (tags == null) {
      return new ArrayList<>();
    }
    for (int i = 0; i < tags.size(); i++) {
      final String tag = tags.get(i);
      if (tag.startsWith(REVIEW_STATE_PREFIX)) {
        tags.remove(i--);
      }
    }
    return tags;
  }

  public static <T> @NotNull T checkInstanceOf(
      final @Nullable Object input,
      final @NotNull Class<T> returnType,
      final @NotNull XyzError xyzError,
      final @NotNull String errDescPrefix) {
    if (input == null) {
      throw new XyzErrorException(xyzError, errDescPrefix + " - object is null.");
    }
    if (returnType.isAssignableFrom(input.getClass())) {
      return returnType.cast(input);
    }
    throw new XyzErrorException(
        xyzError, errDescPrefix + " - " + input.getClass().getSimpleName());
  }

  public static <T> @NotNull T checkInstanceOf(
      final @Nullable Object input, final @NotNull Class<T> returnType, final @NotNull String errDescPrefix) {
    return checkInstanceOf(input, returnType, XyzError.NOT_IMPLEMENTED, errDescPrefix);
  }

  public static void setDeltaReviewState(final @NotNull XyzFeature feature, final @NotNull EReviewState reviewState) {
    final XyzProperties properties = feature.getProperties();
    final XyzNamespace xyzNs = properties.getXyzNamespace();
    final HereDeltaNs deltaNs = properties.useDeltaNamespace();
    deltaNs.setChangeState(EChangeState.UPDATED);
    deltaNs.setReviewState(reviewState);
    final @NotNull List<@NotNull String> tags = tagsWithoutReviewState(xyzNs.getTags());
    tags.add(REVIEW_STATE_PREFIX + reviewState);
    xyzNs.setTags(tags, false);
  }
}
