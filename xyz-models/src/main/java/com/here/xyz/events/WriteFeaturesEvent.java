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

package com.here.xyz.events;

public class WriteFeaturesEvent extends ContextAwareEvent<WriteFeaturesEvent> {
  private UpdateStrategy updateStrategy;
  private byte[] featureData;
  private boolean partialUpdates;

  public UpdateStrategy getUpdateStrategy() {
    return updateStrategy;
  }

  public void setUpdateStrategy(UpdateStrategy updateStrategy) {
    this.updateStrategy = updateStrategy;
  }

  public WriteFeaturesEvent withUpdateStrategy(UpdateStrategy updateStrategy) {
    setUpdateStrategy(updateStrategy);
    return this;
  }

  public byte[] getFeatureData() {
    return featureData;
  }

  public void setFeatureData(byte[] featureData) {
    this.featureData = featureData;
  }

  public WriteFeaturesEvent withFeatureData(byte[] featureData) {
    setFeatureData(featureData);
    return this;
  }

  public boolean isPartialUpdates() {
    return partialUpdates;
  }

  public void setPartialUpdates(boolean partialUpdates) {
    this.partialUpdates = partialUpdates;
  }

  public WriteFeaturesEvent withPartialUpdates(boolean partialUpdates) {
    setPartialUpdates(partialUpdates);
    return this;
  }
}
