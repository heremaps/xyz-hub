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

package com.here.xyz.jobs.steps.inputs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.jobs.util.S3Client;
import java.net.URL;

public class UploadUrl extends Input<UploadUrl> {
  @JsonView(Public.class)
  private long byteSize;
  @JsonView(Public.class)
  private boolean compressed;

  @JsonView(Public.class)
  public URL getUrl() {
    return S3Client.getInstance().generateUploadURL(getS3Key());
  }

  public long getByteSize() {
    return byteSize;
  }

  public void setByteSize(long byteSize) {
    this.byteSize = byteSize;
  }

  public UploadUrl withByteSize(long byteSize) {
    setByteSize(byteSize);
    return this;
  }

  public boolean isCompressed() {
    return compressed;
  }

  public void setCompressed(boolean compressed) {
    this.compressed = compressed;
  }

  public UploadUrl withCompressed(boolean compressed) {
    setCompressed(compressed);
    return this;
  }

  @JsonIgnore
  public long getEstimatedUncompressedByteSize() {
    return compressed ? byteSize * 12 : byteSize;
  }
}
