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
import com.here.xyz.jobs.steps.S3DataFile;
import com.here.xyz.jobs.util.S3Client;
import java.net.URL;

public class UploadUrl extends Input<UploadUrl> implements S3DataFile {

  @JsonView(Public.class)
  public URL getUrl() {
    return S3Client.getInstance(getS3Bucket()).generateUploadURL(getS3Key());
  }

  @JsonView(Public.class)
  public URL getDownloadUrl() {
    return S3Client.getInstance(getS3Bucket()).generateDownloadURL(getS3Key());
  }

  @JsonIgnore
  public long getEstimatedUncompressedByteSize() {
    return isCompressed() ? getByteSize() * 12 : getByteSize();
  }

  @JsonView(Public.class)
  @JsonIgnore(false)
  @Override
  public long getByteSize() {
    return super.getByteSize();
  }

  @JsonView(Public.class)
  @JsonIgnore(false)
  @Override
  public boolean isCompressed() {
    return super.isCompressed();
  }
}
