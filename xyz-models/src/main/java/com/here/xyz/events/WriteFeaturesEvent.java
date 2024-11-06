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

import com.here.xyz.models.geojson.implementation.FeatureCollection;
import java.util.List;
import java.util.Set;

public class WriteFeaturesEvent extends ContextAwareEvent<WriteFeaturesEvent> {
  private Set<Modification> modifications;
  private boolean responseDataExpected;

  public Set<Modification> getModifications() {
    return modifications;
  }

  public void setModifications(Set<Modification> modifications) {
    this.modifications = modifications;
  }

  public WriteFeaturesEvent withModifications(Set<Modification> modifications) {
    setModifications(modifications);
    return this;
  }

  public boolean isResponseDataExpected() {
    return responseDataExpected;
  }

  public void setResponseDataExpected(boolean responseDataExpected) {
    this.responseDataExpected = responseDataExpected;
  }

  public WriteFeaturesEvent withResponseDataExpected(boolean responseDataExpected) {
    setResponseDataExpected(responseDataExpected);
    return this;
  }

  public static class Modification {
    private UpdateStrategy updateStrategy;
    private FeatureCollection featureData;
    private List<String> featureIds; //To be used only for deletions
    private boolean partialUpdates;
    private List<String> featureHooks; //NOTE: The featureHooks will be applied in the given order

    public UpdateStrategy getUpdateStrategy() {
      return updateStrategy;
    }

    public void setUpdateStrategy(UpdateStrategy updateStrategy) {
      this.updateStrategy = updateStrategy;
    }

    public Modification withUpdateStrategy(UpdateStrategy updateStrategy) {
      setUpdateStrategy(updateStrategy);
      return this;
    }

    public FeatureCollection getFeatureData() {
      return featureData;
    }

    public void setFeatureData(FeatureCollection featureData) {
      this.featureData = featureData;
    }

    public Modification withFeatureData(FeatureCollection featureData) {
      setFeatureData(featureData);
      return this;
    }

    public List<String> getFeatureIds() {
      return featureIds;
    }

    public void setFeatureIds(List<String> featureIds) {
      this.featureIds = featureIds;
    }

    public Modification withFeatureIds(List<String> featureIds) {
      setFeatureIds(featureIds);
      return this;
    }

    public boolean isPartialUpdates() {
      return partialUpdates;
    }

    public void setPartialUpdates(boolean partialUpdates) {
      this.partialUpdates = partialUpdates;
    }

    public Modification withPartialUpdates(boolean partialUpdates) {
      setPartialUpdates(partialUpdates);
      return this;
    }

    public List<String> getFeatureHooks() {
      return featureHooks;
    }

    public void setFeatureHooks(List<String> featureHooks) {
      this.featureHooks = featureHooks;
    }

    public Modification withFeatureHooks(List<String> featureHooks) {
      setFeatureHooks(featureHooks);
      return this;
    }
  }
}
