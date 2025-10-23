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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.S3DataFile;
import com.here.xyz.jobs.util.S3Client;
import java.io.IOException;
import java.net.URL;

public class DownloadUrl extends Output<DownloadUrl> implements S3DataFile {
  @JsonView(Public.class)
  private long byteSize;
  @JsonIgnore
  private byte[] content;
  private String contentType = "application/octet-stream";

  @Override
  public void store(String s3Key) throws IOException {
    if (content == null)
      throw new IllegalStateException("No content was provided for the output to be stored.");
    S3Client.getInstance().putObject(s3Key, contentType, content);
  }

  @JsonView(Public.class)
  public URL getUrl() {
    return S3Client.getInstance().generateDownloadURL(getS3Key());
  }

  @Override
  public boolean isCompressed() {
    if (this.getS3Key() != null) {
      return this.getS3Key().endsWith(".zip");
    }
    return false;
  }

  public long getByteSize() {
    return byteSize;
  }

  public void setContent(byte[] content) {
    this.content = content;
  }

  public DownloadUrl withContent(byte[] content) {
    setContent(content);
    return this;
  }

  public void setContentType(String contentType) {
    this.contentType = contentType;
  }

  public DownloadUrl withContentType(String contentType) {
    setContentType(contentType);
    return this;
  }

  @Override
  @JsonIgnore
  public String getS3Bucket() {
    //Current outputs are written to default bucket only
    return Config.instance.JOBS_S3_BUCKET;
  }

  @Override
  @JsonIgnore
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
