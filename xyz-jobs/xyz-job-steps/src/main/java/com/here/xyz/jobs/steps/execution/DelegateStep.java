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

package com.here.xyz.jobs.steps.execution;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.here.xyz.jobs.RuntimeInfo;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.StepExecution;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import java.util.List;

public class DelegateStep extends Step<DelegateStep> {
  @JsonIgnore
  private Step<?> delegate;
  @JsonIgnore
  private Step<?> delegator;
  private RuntimeInfo status = new RuntimeInfo();

  public Step<?> getDelegate() {
    return delegate;
  }

  public void setDelegate(Step<?> delegate) {
    this.delegate = delegate;
    setOutputMetadata(delegate.getOutputMetadata());
    outputSets = delegate.getOutputSets();
  }

  public DelegateStep withDelegate(Step<?> delegate) {
    setDelegate(delegate);
    return this;
  }

  public Step<?> getDelegator() {
    return delegator;
  }

  public void setDelegator(Step<?> delegator) {
    this.delegator = delegator;
  }

  public DelegateStep withDelegator(Step<?> delegator) {
    setDelegator(delegator);
    return this;
  }

  @Override
  public String getJobId() {
    if (getDelegator() == null)
      return "delegator";
    return getDelegator().getJobId();
  }

  @Override
  public String getId() {
    if (getDelegator() == null)
      return "delegator";
    return getDelegator().getId();
  }

  @Override
  public RuntimeInfo getStatus() {
    return status;
  }

  @Override
  public List<Load> getNeededResources() {
    return List.of();
  }

  @Override
  public int getTimeoutSeconds() {
    return 0;
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    return 0;
  }

  @Override
  public String getDescription() {
    return "A pseudo step that just delegates outputs of a Step of another StepGraph into this StepGraph";
  }

  @Override
  public void execute() throws Exception {
    throw new IllegalAccessException("This method should never be called");
  }

  @Override
  public void resume() throws Exception {
    execute();
  }

  @Override
  public void cancel() throws Exception {
    throw new IllegalAccessException("This method should never be called");
  }

  @Override
  public boolean validate() throws ValidationException {
    return true;
  }

  @Override
  public void deleteOutputs() {
    //TODO: Implement logic for checking the reference counter here?
  }

  @Override
  public boolean isEquivalentTo(StepExecution other) {
    return getDelegate().isEquivalentTo(other);
  }
}
