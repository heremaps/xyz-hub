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

package com.here.xyz.jobs.steps.inputs;

import com.here.xyz.jobs.steps.outputs.Output;

//TODO: Remove that workaround once the inputs & outputs were streamlined to one single inheritance chain
public class InputFromOutput extends ModelBasedInput {
  private Output delegate;

  public Output getDelegate() {
    return delegate;
  }

  public void setDelegate(Output delegate) {
    this.delegate = delegate;
  }

  public InputFromOutput withDelegate(Output delegate) {
    setDelegate(delegate);
    setMetadata(delegate.getMetadata());
    return this;
  }
}
