/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

package com.here.xyz.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ChangesetEvent extends Event<ChangesetEvent> {
  private Operation operation;
  private PropertyQuery historyVersion;

  public Operation getOperation() {
    return operation;
  }

  public void setOperation(Operation operation) {
    this.operation = operation;
  }

  public ChangesetEvent withOperation(Operation operation) {
    setOperation(operation);
    return this;
  }

  public PropertyQuery getHistoryVersion() {
    return historyVersion;
  }

  public void setHistoryVersion(PropertyQuery historyVersion) {
    this.historyVersion = historyVersion;
  }

  public ChangesetEvent withHistoryVersion(PropertyQuery historyVersion) {
    setHistoryVersion(historyVersion);
    return this;
  }

  public enum Operation {
    DELETE
  }
}
