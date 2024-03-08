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

package com.here.xyz.jobs;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.here.xyz.jobs.steps.S3Client;
import com.here.xyz.util.Async;
import com.here.xyz.util.service.Core;
import io.vertx.core.Future;
import java.util.List;

public class AsyncS3Client extends S3Client {
  private static final AsyncS3Client instance = new AsyncS3Client(Config.instance.JOBS_S3_BUCKET);
  private Async async = new Async(20, Core.vertx, AsyncS3Client.class);

  protected AsyncS3Client(String bucketName) {
    super(bucketName);
  }

  public static AsyncS3Client getInstance() {
    return instance;
  }

  //NOTE: Only the long-blocking methods are added as async variants

  public Future<Void> deleteS3FolderAsync(String folderPath) {
    return async.run(() -> {
      deleteS3Folder(folderPath);
      return null;
    });
  }

  public Future<List<S3ObjectSummary>> scanFolderAsync(String folderPath) {
    return async.run(() -> scanFolder(folderPath));
  }
}
