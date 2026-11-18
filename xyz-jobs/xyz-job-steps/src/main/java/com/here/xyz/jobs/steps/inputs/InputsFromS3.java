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

package com.here.xyz.jobs.steps.inputs;

import com.here.xyz.jobs.util.S3Client;
import com.here.xyz.util.service.aws.s3.S3Uri;
import java.util.List;
import java.util.function.Supplier;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class InputsFromS3 extends Input<InputsFromS3> {
  private String bucket;
  private String prefix;

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix(String prefix) {
    if (prefix.startsWith("/"))
      prefix = prefix.substring(1);
    this.prefix = prefix;
  }

  public InputsFromS3 withPrefix(String prefix) {
    setPrefix(prefix);
    return this;
  }

  public String getBucket() {
    return bucket;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public InputsFromS3 withBucket(String bucket) {
    setBucket(bucket);
    return this;
  }

  public void dereference(String forJob, String setName) {
    //First load the inputs from the (foreign) bucket
    List<Input> inputs = mapAccessDenied(() -> loadInputsInParallel(getBucket(), getPrefix()));
    inputs.forEach(input -> input.setS3Bucket(getBucket()));
    //Store the metadata for the job that accesses the bucket
    storeMetadata(forJob, inputs, null, new S3Uri(getBucket(), getPrefix()), setName);
  }

  /**
   * Lists at most one object below the prefix to check whether the service may list the bucket at all.
   * In contrast to {@link #dereference(String, String)}, this takes a single S3 request, independent of the number of objects.
   *
   * @throws BucketAccessDeniedException if the bucket policy does not grant the service the access
   */
  public void checkAccess() {
    mapAccessDenied(() -> S3Client.getInstance(getBucket()).scanFolder(getPrefix(), null, 1));
  }

  private <R> R mapAccessDenied(Supplier<R> bucketAccess) {
    try {
      return bucketAccess.get();
    }
    catch (S3Exception e) {
      if (e.statusCode() == 403)
        throw new BucketAccessDeniedException(new S3Uri(getBucket(), getPrefix()), e);
      throw e;
    }
  }

  /**
   * Thrown if the service is not allowed to access the provided bucket, e.g. because its bucket policy does not grant the access.
   */
  public static class BucketAccessDeniedException extends RuntimeException {
    public BucketAccessDeniedException(S3Uri s3Uri, S3Exception cause) {
      super("Access to \"" + s3Uri + "\" was denied: " + cause.getMessage(), cause);
    }
  }
}
