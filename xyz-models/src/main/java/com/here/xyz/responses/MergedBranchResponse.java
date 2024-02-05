/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.responses;

import com.here.xyz.models.hub.Ref;

public class MergedBranchResponse extends ModifiedBranchResponse {
  private long mergedSourceVersion;
  private Ref resolvedMergeTargetRef;

  public long getMergedSourceVersion() {
    return mergedSourceVersion;
  }

  public void setMergedSourceVersion(long mergedSourceVersion) {
    this.mergedSourceVersion = mergedSourceVersion;
  }

  public MergedBranchResponse withMergedSourceVersion(long mergedSourceVersion) {
    setMergedSourceVersion(mergedSourceVersion);
    return this;
  }

  public Ref getResolvedMergeTargetRef() {
    return resolvedMergeTargetRef;
  }

  public void setResolvedMergeTargetRef(Ref resolvedMergeTargetRef) {
    this.resolvedMergeTargetRef = resolvedMergeTargetRef;
  }

  public MergedBranchResponse withResolvedMergeTargetRef(Ref resolvedMergeTargetRef) {
    setResolvedMergeTargetRef(resolvedMergeTargetRef);
    return this;
  }
}
