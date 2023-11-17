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
package com.here.naksha.lib.psql;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.here.naksha.lib.core.models.storage.EStorageOp;
import com.here.naksha.lib.core.models.storage.FeatureCodec;
import org.jetbrains.annotations.NotNull;

/**
 * Internally used object to serialize operations for invoking the database pl/pgsql functions.
 */
class PostgresWriteOp {
  @JsonProperty("op")
  EStorageOp op;

  @JsonProperty("id")
  String id;

  @JsonProperty("uuid")
  String uuid;

  @JsonRawValue
  @JsonProperty("feature")
  String feature;

  @JsonIgnore
  <FEATURE, CODEC extends FeatureCodec<FEATURE, CODEC>> @NotNull PostgresWriteOp decode(@NotNull CODEC codec) {
    codec.decodeParts();
    this.op = codec.getOp();
    this.id = codec.getId();
    this.uuid = codec.getUuid();
    this.feature = codec.getJson();
    return this;
  }
}
