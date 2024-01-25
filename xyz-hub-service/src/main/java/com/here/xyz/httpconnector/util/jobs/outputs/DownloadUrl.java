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

package com.here.xyz.httpconnector.util.jobs.outputs;

import java.net.URL;

public class DownloadUrl extends Output<DownloadUrl> {
  private URL url;
  private long byteSize;

  public URL getUrl() {
    return url;
  }

  public void setUrl(URL url) {
    this.url = url;
  }

  public DownloadUrl withUrl(URL url) {
    setUrl(url);
    return this;
  }

  public long getByteSize() {
    return byteSize;
  }

  public void setByteSize(long byteSize) {
    this.byteSize = byteSize;
  }

  public DownloadUrl withByteSize(long byteSize) {
    setByteSize(byteSize);
    return this;
  }
}
