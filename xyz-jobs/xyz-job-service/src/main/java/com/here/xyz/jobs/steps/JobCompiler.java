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

package com.here.xyz.jobs.steps;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.steps.compiler.ExportToFiles;
import com.here.xyz.jobs.steps.compiler.ImportFromFiles;
import com.here.xyz.jobs.steps.compiler.JobCompilationInterceptor;
import com.here.xyz.jobs.steps.compiler.SpaceCopy;
import com.here.xyz.util.Async;
import io.vertx.core.Future;
import io.vertx.core.impl.ConcurrentHashSet;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class JobCompiler {

  private static Set<Class<? extends JobCompilationInterceptor>> interceptors = new ConcurrentHashSet<>();
  private static final Async ASYNC = new Async(5, JobCompiler.class);

  static {
    registerCompilationInterceptor(ImportFromFiles.class);
    registerCompilationInterceptor(ExportToFiles.class);
    registerCompilationInterceptor(SpaceCopy.class);
  }

  public Future<StepGraph> compile(Job job) {
    //First identify the correct compilation interceptor
    List<JobCompilationInterceptor> interceptorCandidates = new LinkedList<>();
    List<CompilationError> errors = new LinkedList<>();
    for (Class<? extends JobCompilationInterceptor> interceptor : interceptors) {
      JobCompilationInterceptor interceptorInstance;
      try {
        interceptorInstance = interceptor.getDeclaredConstructor().newInstance();
        if (interceptorInstance.chooseMe(job))
          interceptorCandidates.add(interceptorInstance);
      }
      catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        errors.add(new CompilationError("Error instantiating compilation interceptor " + interceptor.getSimpleName(), e));
      }
    }

    if (interceptorCandidates.isEmpty())
      throw new CompilationError("Job \"" + job.getId() + "\" is not supported. No according compilation interceptor was found.",
          errors);

    if (interceptorCandidates.size() > 1)
      throw new UnexpectedCompilerError("Job \"" + job.getId() + "\" can not be compiled due to ambiguity. "
          + "Multiple compilation interceptors were found: "
          + interceptorCandidates.stream().map(c -> c.getClass().getSimpleName()).collect(Collectors.joining(", ")));

    /*
    NOTE:
    It's necessary to run the compilation on an async pool,
    because some compilation interceptors call step methods that might take some time to be evaluated.
     */
    return ASYNC.run(() -> interceptorCandidates.get(0).compile(job).enrich(job.getId()))
        .map(graph -> {
          //Pass the info to all steps whether they belong to a "pipeline-job" or not
          graph.stepStream().forEach(step -> step.withPipeline(job.isPipeline()));
          return graph;
        });
  }

  public static JobCompiler getInstance() {
    return new JobCompiler();
  }

  public static void registerCompilationInterceptor(Class<? extends JobCompilationInterceptor> interceptor) {
    interceptors.add(interceptor);
  }

  public static void deregisterCompilationInterceptor(Class<? extends JobCompilationInterceptor> interceptor) {
    interceptors.remove(interceptor);
  }

  public static class UnexpectedCompilerError extends RuntimeException {

    public UnexpectedCompilerError(String message) {
      super(message);
    }

    public UnexpectedCompilerError(String message, Throwable cause) {
      super(message, cause);
    }
  }

  public static class CompilationError extends IllegalArgumentException {
    private List<CompilationError> otherErrors;

    public CompilationError(String message) {
      super(message);
    }

    public CompilationError(String message, Throwable cause) {
      super(message, cause);
    }

    public CompilationError(String message, List<CompilationError> otherErrors) {
      super(message, otherErrors.isEmpty() ? null : otherErrors.get(0));
      this.otherErrors = otherErrors;
    }
  }
}
