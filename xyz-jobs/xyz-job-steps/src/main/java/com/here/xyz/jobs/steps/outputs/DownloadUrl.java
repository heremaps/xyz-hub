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

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.S3DataFile;
import com.here.xyz.jobs.util.S3Client;
import java.net.URL;

public class DownloadUrl extends Output<DownloadUrl> implements S3DataFile {
  @JsonView(Public.class)
  private long byteSize;

  @Override
  public void store(String s3Key) {
    /*
    NOTE: Nothing to do here for now, later (for some step implementations it could be usable if we implement S3 upload logic for binaries here)
    However, for now all step implementations care about uploading binaries to S3 by themselves (e.g. EMR, DB related steps)
     */
  }

  @JsonView(Public.class)
  public URL getUrl() {
    return S3Client.getInstance().generateDownloadURL(getS3Key());
  }

  @Override
  public boolean isCompressed() {
    return false;
  }

  public long getByteSize() {
    return byteSize;
  }

  @Override
  public String getS3Bucket() {
    //Current outputs are written to default bucket only
    return Config.instance.JOBS_S3_BUCKET;
  }

  @Override
  public long getEstimatedUncompressedByteSize() {
    if (isCompressed())
      throw new RuntimeException("Not Implemented: Compression for outputs is currently not supported.");
    return getByteSize();
  }

  public void setByteSize(long byteSize) {
    this.byteSize = byteSize;
  }

  public DownloadUrl withByteSize(long byteSize) {
    setByteSize(byteSize);
    return this;
  }

  @Override
  protected boolean hasMetadata() {
    //For all instances created by subclasses of DownloadUrl, extra fields could be expected
    return !this.getClass().equals(DownloadUrl.class);
  }
}
