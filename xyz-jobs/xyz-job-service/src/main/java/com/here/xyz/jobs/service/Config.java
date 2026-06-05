/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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
import java.util.Map;
import java.util.stream.Collectors;

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
   * The ARN of the DynamoDB table for jobs
   */
  public String JOBS_DYNAMODB_TABLE_ARN;

  /**
   * ARN of the DynamoDB table for job to resource-key relations.
   */
  public String RESOURCE_KEYS_DYNAMODB_TABLE_ARN;

  /**
   * The ARN of the step lambda being called by the step functions
   */
  public ARN STEP_LAMBDA_ARN;

  /**
   * The lambda alias of the step lambda to be used for steps belonging to a pipeline job
   */
  public String STEP_LAMBDA_PIPELINE_ALIAS;

  /**
   * Additional target mappings for specific step classes.
   * Specific step classes can be "routed" to other Lambda Functions than the default one.
   * This configuration setting contains one entry per step class (specified by full qualified class name) to be "re-routed" and defines
   * the target Lambda ARN and the ARN of the invoker's role to be used for invoking the Lambda for that step class.
   *
   * The expected format is a JSON list of objects of which each is looking like:
   * {@code {"fullQualifiedStepClassName": "com.here.xyz.jobs.steps.impl.MySpecialStep", "stepLambdaArn": "arn:aws:lambda:...", "stepLambdaInvocationRoleArn": "arn:aws:iam::...:role/..."}}
   * The fullQualifiedStepClassName has to be unique across the list.
   *  (NOTE: Only subclasses of {@link com.here.xyz.jobs.steps.execution.LambdaBasedStep} can be specified and all subclasses
   *  of the specified class will be affected)
   * The stepLambdaArn has to be an ARN of a Lambda function.
   * The stepLambdaInvocationRoleArn has to be an ARN of an IAM role which the step function will assume when invoking
   *  the Lambda function for the respective step class. This role needs to have the necessary permissions to invoke the Lambda function.
   *
   * The stepLambdaArn and stepLambdaInvocationRoleArn values will override the default STEP_LAMBDA_ARN and the default invocation role
   * for steps of the respective step class.
   * If a step class is not listed here, the default STEP_LAMBDA_ARN and the default invocation role will be used for steps of that class.
   */
  public List<StepLambdaMapping> ALTERNATIVE_STEP_LAMBDA_MAPPINGS;

  private Map<String, StepLambdaMapping> alternativeStepLambdaMappings;

  public Map<String, StepLambdaMapping> getAlternativeStepLambdaMappings() {
    if (alternativeStepLambdaMappings == null)
      alternativeStepLambdaMappings = ALTERNATIVE_STEP_LAMBDA_MAPPINGS == null ? Map.of() : ALTERNATIVE_STEP_LAMBDA_MAPPINGS.stream()
          .collect(Collectors.toMap(StepLambdaMapping::fullQualifiedStepClassName, mapping -> mapping));

    return alternativeStepLambdaMappings;
  }

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

  public record StepLambdaMapping(String fullQualifiedStepClassName, ARN stepLambdaArn, ARN stepLambdaInvocationRoleArn) {}
}
