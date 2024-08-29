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

package com.here.xyz.jobs.service;

import com.here.xyz.util.ARN;
import java.util.List;

public class Config extends com.here.xyz.jobs.steps.Config {
  public static Config instance;

  {
    instance = this;
  }

  /**
   * The router builder class names, separated by comma
   */
  public String ROUTER_BUILDER_CLASS_NAMES;

  /**
   * ARN of DynamoDB Table for JOBs
   */
  public String JOBS_DYNAMODB_TABLE_ARN;

  /**
   * The ARN of the step lambda being called by the step functions
   */
  public ARN STEP_LAMBDA_ARN;

  /**
   * The ARN of the role needed for step function
   */
  public String STATE_MACHINE_ROLE;

  /**
   * Whether steps in a StepGraph can be executed in parallel at all within the target environment.
   */
  public boolean PARALLEL_STEPS_SUPPORTED = true;

  /**
   * A string that contains the full qualified class names of Job plugins, separated by comma
   */
  public String JOB_PLUGINS;

  public List<String> jobPlugins() {
    return fromCommaSeparatedList(JOB_PLUGINS);
  }
}
