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

import java.util.Map;

public class OneTimeActionEvent extends Event<OneTimeActionEvent> {
  private String phase;
  private Map<String, Object> inputData;

  public String getPhase() {
    return phase;
  }

  public void setPhase(String phase) {
    this.phase = phase;
  }

  public OneTimeActionEvent withPhase(String phase) {
    setPhase(phase);
    return this;
  }

  public Map<String, Object> getInputData() {
    return inputData;
  }

  public void setInputData(Map<String, Object> inputData) {
    this.inputData = inputData;
  }

  public OneTimeActionEvent withInputData(Map<String, Object> inputData) {
    setInputData(inputData);
    return this;
  }
}
