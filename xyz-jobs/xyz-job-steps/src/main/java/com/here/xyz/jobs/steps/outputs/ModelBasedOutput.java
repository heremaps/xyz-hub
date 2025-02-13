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

package com.here.xyz.jobs.steps.outputs;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.util.S3Client;
import java.io.IOException;
import java.util.Map;

@JsonTypeInfo(use = Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = FeatureStatistics.class, name = "FeatureStatistics"),
    @JsonSubTypes.Type(value = CreatedVersion.class, name = "CreatedVersion"),
    @JsonSubTypes.Type(value = TileInvalidations.class, name = "TileInvalidationList")
})
public abstract class ModelBasedOutput extends Output<ModelBasedOutput> {
  @Override
  public void store(String s3Key) throws IOException {
      S3Client.getInstance().putObject(s3Key, "application/json", serialize());
  }

  public static ModelBasedOutput load(String s3Key, Map<String, String> metadata) {
    try {
      return XyzSerializable.deserialize(S3Client.getInstance().loadObjectContent(s3Key), ModelBasedOutput.class)
              .withMetadata(metadata);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
