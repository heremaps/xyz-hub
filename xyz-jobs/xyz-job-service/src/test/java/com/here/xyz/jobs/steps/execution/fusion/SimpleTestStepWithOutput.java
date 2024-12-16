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

package com.here.xyz.jobs.steps.execution.fusion;

import static com.here.xyz.jobs.steps.Step.Visibility.USER;

import java.util.List;

public class SimpleTestStepWithOutput extends SimpleTestStep<SimpleTestStepWithOutput> {
  public static final String SOME_OUTPUT = "someOutput";

  {
    //Define which outputs are produced by this step
    setOutputSets(List.of(new OutputSet(SOME_OUTPUT, USER, false)));
  }

  public SimpleTestStepWithOutput(String paramA, String paramB, String paramC) {
    super(paramA, paramB, paramC);
  }

  public SimpleTestStepWithOutput(String paramA) {
    super(paramA);
  }

  @Override
  public String getDescription() {
    return "A simple test step with output.";
  }
}
