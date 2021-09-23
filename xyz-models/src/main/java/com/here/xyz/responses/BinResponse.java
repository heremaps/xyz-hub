/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

/**
 * @deprecated Not to be used in protocol versions >=0.6.0, instead {@link BinaryResponse} should be used.
 * This class will be removed in future versions of this library.
 */
@Deprecated
public class BinResponse extends XyzResponse<BinResponse> {

  private byte[] bytes;

  public byte[] getBytes() {
    return bytes;
  }

  public void setBytes(byte[] bytes) {
    this.bytes = bytes;
  }

  public BinResponse withBytes(byte[] bytes) {
    setBytes(bytes);
    return this;
  }
}
