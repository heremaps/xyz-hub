/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.xyz.hub.params;

import java.io.Serializable;

/**
 * The part to return of a result-set. Virtually split the result-set into {@link #total} parts and returns the part number {@link #part}
 * with {@link #part} being a value between {@code 1} and {@link #total}.
 */
public class XyzPart implements Serializable {

  public XyzPart(int part, int total) {
    this.part = part;
    this.total = total;
  }

  /**
   * The part to return, must be greater than zero and less than or equal {@link #total}.
   */
  public final int part;

  /**
   * The total amount of parts into which to split a result-set.
   */
  public final int total;
}
