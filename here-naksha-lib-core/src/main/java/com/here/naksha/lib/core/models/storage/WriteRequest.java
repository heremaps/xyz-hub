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
package com.here.naksha.lib.core.models.storage;

import static com.here.naksha.lib.core.models.storage.EWriteOp.CREATE;
import static com.here.naksha.lib.core.models.storage.EWriteOp.DELETE;
import static com.here.naksha.lib.core.models.storage.EWriteOp.PURGE;
import static com.here.naksha.lib.core.models.storage.EWriteOp.PUT;
import static com.here.naksha.lib.core.models.storage.EWriteOp.UPDATE;

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * All write requests should extend this base class and follow the principles described here.
 *
 * <p>The storage may re-order the feature codecs before execution, so execute them in a different order, if needed. The response to a
 * {@code WriteFeatures} request will be either a {@link SuccessResult} or a {@link ErrorResult}. If no error happened, the storage will
 * return a {@link SuccessResult}, if any error happened, it will return an {@link ErrorResult}.
 *
 * <p>In the cases of a {@link SuccessResult} the storage <b>must</b> return a {@link ForwardCursor cursor} to review the results. For an
 * {@link ErrorResult} the storage <b>must</b>> return a {@link ForwardCursor cursor} only, if the request succeeded partially. It will not
 * return a {@link ForwardCursor cursor}, if the whole {@link WriteRequest} failed.
 *
 * <p>If a {@link ForwardCursor cursor} is returned, for every write operation at least one result will be available. Currently only the
 * {@link EWriteOp#PURGE} operation may result in two results being returned. All results will return an {@link EExecutedOp}, clarifying the
 * operation that was performed. The details about what each execution means for the result can be read at the {@link EExecutedOp}
 * documentation.
 *
 * @param <FEATURE> The feature-type to write.
 * @param <CODEC>   The codec to use to encode features.
 * @param <SELF>    The self-type.
 */
@AvailableSince(NakshaVersion.v2_0_7)
public abstract class WriteRequest<
        FEATURE, CODEC extends FeatureCodec<FEATURE, CODEC>, SELF extends WriteRequest<FEATURE, CODEC, SELF>>
    extends Request<SELF> {

  /**
   * Creates a new abstract write request.
   *
   * @param codecFactory The codec factory to use when creating new feature codecs.
   */
  @AvailableSince(NakshaVersion.v2_0_11)
  protected WriteRequest(
      final @NotNull FeatureCodecFactory<FEATURE, CODEC> codecFactory,
      final @NotNull List<@NotNull CODEC> features) {
    this.codecFactory = codecFactory;
    this.features = features;
  }

  /**
   * Creates a new abstract write request.
   *
   * @param codecFactory The codec factory to use when creating new feature codecs.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  protected WriteRequest(@NotNull FeatureCodecFactory<FEATURE, CODEC> codecFactory) {
    this.codecFactory = codecFactory;
    this.features = new ArrayList<>();
  }

  /**
   * The codec factory to use, when adding new features.
   */
  protected @NotNull FeatureCodecFactory<FEATURE, CODEC> codecFactory;

  /**
   * The features wrapped into codecs to allow encoding and decoding for the storage.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull List<@NotNull CODEC> features;

  /**
   * If the result-cursor should not hold the final feature and geometry, this saves IO, but does not provide back details about the new
   * {@link XyzNamespace} that was generated.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public boolean minResults;

  /**
   * Add a feature and operation.
   *
   * @param op the operation to perform.
   * @return this.
   */
  public @NotNull SELF add(@NotNull EWriteOp op, @NotNull FEATURE feature) {
    CODEC codec = codecFactory.newInstance();
    codec.setOp(op);
    codec.setFeature(feature);
    features.add(codec);
    return self();
  }

  public @NotNull SELF create(@NotNull FEATURE feature) {
    return add(CREATE, feature);
  }

  public @NotNull SELF put(@NotNull FEATURE feature) {
    return add(PUT, feature);
  }

  public @NotNull SELF update(@NotNull FEATURE feature) {
    return add(UPDATE, feature);
  }

  public @NotNull SELF delete(@NotNull FEATURE feature) {
    return add(DELETE, feature);
  }

  public @NotNull SELF delete(@NotNull String id, @Nullable String uuid) {
    CODEC codec = codecFactory.newInstance();
    codec.setOp(DELETE);
    codec.setId(id);
    codec.setUuid(uuid);
    codec.isDecoded = true;
    features.add(codec);
    return self();
  }

  public @NotNull SELF purge(@NotNull FEATURE feature) {
    return add(PURGE, feature);
  }

  public @NotNull SELF purge(@NotNull String id, @Nullable String uuid) {
    CODEC codec = codecFactory.newInstance();
    codec.setOp(PURGE);
    codec.setId(id);
    codec.setUuid(uuid);
    codec.isDecoded = true;
    features.add(codec);
    return self();
  }

  public FeatureCodecFactory<FEATURE, CODEC> getCodecFactory() {
    return codecFactory;
  }
}
