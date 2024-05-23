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

package com.here.xyz.jobs.steps.compiler;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.JobCompiler.CompilationError;
import com.here.xyz.jobs.steps.StepGraph;

public interface JobCompilationInterceptor {

  /**
   * This method decides whether the specified job is to be compiled by this compilation interceptor or not.
   * This method must be implemented in a way that two subsequent calls for equal job configurations will result
   * in the same return value.
   * Returning <code>true</code> means, that this compilation interceptor should be chosen by the root job compiler
   * to perform the actual compilation for the specified job. In that case the job compiler will call
   * the method {@link #compile(Job)} one or multiple times afterwards.
   *
   * Taking the decision on whether this interceptor is responsible for the job's compilation should be
   * done on the basis of the information being provided within the job's source & target definitions.
   * These definitions can be retrieved from the job directly using the methods:
   * - {@link Job#getSource()}
   * - {@link Job#getTarget()}
   * The needed information is primarily the type of the source / target objects and their according settings parameters.
   *
   * NOTE: Within the whole framework, there might be always only *one* compilation interceptor that may be responsible
   * for compiling a specific job. That means the implementation of this method *must* assure return <code>false</code>
   * for all cases in which it is not responsible for the specified job. Otherwise, the job will fail with a {@link CompilationError}.
   *
   * @param job The job to be compiled into a {@link StepGraph}
   * @return Whether this interceptor is responsible for the compilation of the specified job
   */
  boolean chooseMe(Job job);

  /**
   * Will be called by the root job compiler to perform the actual compilation of the job
   * if this compilation interceptor was chosen to be responsible for the specified job.
   *
   * Compiling a job means reading its source & target definitions and creating the according {@link StepGraph} that
   * depicts the flow of sequential and/or parallel steps to be executed for that job.
   *
   * @param job The job to be compiled into a {@link StepGraph}
   * @return The {@link StepGraph} describing the flow of steps for the specified job
   */
  CompilationStepGraph compile(Job job);
}
