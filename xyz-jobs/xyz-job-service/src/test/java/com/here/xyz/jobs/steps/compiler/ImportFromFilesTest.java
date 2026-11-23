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

package com.here.xyz.jobs.steps.compiler;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.jobs.datasets.files.FileFormat.EntityPerLine.Feature;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.filters.Filters;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.JobTest;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.datasets.files.FileInputSettings;
import com.here.xyz.jobs.datasets.files.GeoJson;
import com.here.xyz.jobs.service.Config;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.impl.DropIndexes;
import com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace;
import com.here.xyz.models.hub.Space;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Compiler tests for {@link ImportFromFiles} that verify a user-provided {@code context} filter on the
 * import target (e.g. {@code context=EXTENSION}) is recognized during compilation and forwarded to the
 * resulting {@link TaskedImportFilesToSpace} step and its selected writer.
 */
public class ImportFromFilesTest extends JobTest {

  private final String SPACE_ID_EXT = SPACE_ID + "_ext";

  /**
   * The index wrapper of an import into an empty layer reads the job service config. Creating it would also replace the step config
   * of the test base, which points to the local services.
   */
  @BeforeAll
  public static void setUpServiceConfig() {
    com.here.xyz.jobs.steps.Config stepConfig = com.here.xyz.jobs.steps.Config.instance;
    if (Config.instance == null)
      new Config();
    com.here.xyz.jobs.steps.Config.instance = stepConfig;
  }

  @BeforeEach
  public void setUp() {
    //Base space (the "super" space of the composite)
    createSpace(new Space().withId(SPACE_ID).withVersionsToKeep(10), false);
    //Composite space extending the base space -> import into it uses a staged writer
    createSpace(new Space().withId(SPACE_ID_EXT).withVersionsToKeep(10)
        .withExtension(new Space.Extension().withSpaceId(SPACE_ID)), false);
  }

  /**
   * When the user specifies {@code context=EXTENSION} on the target, the compiled import step must carry
   * that context so that it gets passed to the selected writer.
   */
  @Test
  public void testImportWithContextExtension() {
    TaskedImportFilesToSpace importStep = compileAndGetImportStep(EXTENSION);
    Assertions.assertEquals(EXTENSION, importStep.getContext(),
        "context=EXTENSION must be forwarded from the target filters to the import step");
  }

  /**
   * When the user explicitly specifies {@code context=DEFAULT} on the target, the compiled import step must
   * carry the DEFAULT context.
   */
  @Test
  public void testImportWithContextDefault() {
    TaskedImportFilesToSpace importStep = compileAndGetImportStep(DEFAULT);
    Assertions.assertEquals(DEFAULT, importStep.getContext(),
        "context=DEFAULT must be forwarded from the target filters to the import step");
  }

  /**
   * When no filters/context are provided on the target, the compiled import step must not carry any context
   * (the selected writer then falls back to its DEFAULT behaviour).
   */
  @Test
  public void testImportWithoutContext() {
    TaskedImportFilesToSpace importStep = compileAndGetImportStep(null);
    Assertions.assertNull(importStep.getContext(),
        "No context must be set on the import step when none was provided on the target");
  }

  /**
   * An import into an empty layer is wrapped with the drop and re-creation of the indices. The DropIndexes step of that wrapper
   * creates the table of a lazy target without its indices, which the wrapper creates afterwards.
   */
  @Test
  public void testDropIndexesMaterializesLazyTable() {
    List<DropIndexes> dropIndexSteps = compile(SPACE_ID, null).stepStream()
        .filter(step -> step instanceof DropIndexes)
        .map(step -> (DropIndexes) step)
        .toList();
    Assertions.assertEquals(1, dropIndexSteps.size());
    Assertions.assertTrue(dropIndexSteps.get(0).isMaterializeLazyTable());
  }

  private TaskedImportFilesToSpace compileAndGetImportStep(SpaceContext context) {
    return (TaskedImportFilesToSpace) compile(SPACE_ID_EXT, context).stepStream()
        .filter(step -> step instanceof TaskedImportFilesToSpace)
        .findFirst()
        .orElseThrow(() -> new AssertionError("No TaskedImportFilesToSpace step found in compiled graph"));
  }

  private CompilationStepGraph compile(String spaceId, SpaceContext context) {
    DatasetDescription.Space<?> target = new DatasetDescription.Space<>().withId(spaceId);
    if (context != null)
      target.setFilters(new Filters().withContext(context));

    return new ImportFromFiles().compile(buildImportJob(target));
  }

  private Job buildImportJob(DatasetDescription target) {
    return new Job()
        .withId(JOB_ID)
        .withDescription("Import Job Context Test")
        .withSource(new Files<>().withInputSettings(
            new FileInputSettings().withFormat(new GeoJson().withEntityPerLine(Feature))))
        .withTarget(target);
  }
}
