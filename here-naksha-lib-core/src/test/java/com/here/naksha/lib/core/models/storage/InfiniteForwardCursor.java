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
package com.here.naksha.lib.core.models.storage;

import java.util.UUID;
import org.jetbrains.annotations.NotNull;

public class InfiniteForwardCursor<FEATURE, CODEC extends FeatureCodec<FEATURE, CODEC>>
    extends ForwardCursor<FEATURE, CODEC> {

  private String json = "{\"type\":\"Feature\"}";
  private int lastId = 0;

  protected InfiniteForwardCursor(@NotNull FeatureCodecFactory<FEATURE, CODEC> codecFactory) {
    super(codecFactory);
  }

  protected InfiniteForwardCursor(@NotNull FeatureCodecFactory<FEATURE, CODEC> codecFactory, String json) {
    super(codecFactory);
    this.json = json;
  }

  @Override
  protected boolean loadNextRow(ForwardCursor<FEATURE, CODEC>.@NotNull Row row) {
    row.codec.setOp("CREATED");
    row.codec.setId("id:" + lastId);
    row.codec.setUuid(UUID.randomUUID().toString());
    row.codec.setFeatureType("Feature");
    row.codec.setPropertiesType(null);
    row.codec.setJson(json);
    row.codec.setWkb(null);
    row.codec.setRawError(null);
    row.codec.setErr(null);
    row.valid = true;

    lastId++;
    return true;
  }

  @Override
  public void close() {}
}
