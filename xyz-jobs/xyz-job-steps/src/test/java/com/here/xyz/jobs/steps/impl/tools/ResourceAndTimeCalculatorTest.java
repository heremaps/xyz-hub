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

package com.here.xyz.jobs.steps.impl.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

/**
 * Covers the ACU estimation of an import, in particular that the estimate grows linearly with the configured
 * concurrency.
 *
 * <p>It used to grow quadratically, because the thread count was applied twice: once to derive the byte size handed
 * to {@code calculateNeededAcusFromByteSize} and once as a factor on its result. For small files that claimed far
 * more than a step can use, and for larger ones it simply saturated the cap, which hid the effect.</p>
 */
public class ResourceAndTimeCalculatorTest {

  private static final long GB = 1024L * 1024 * 1024;
  /** Each ACU provides 2 GB of RAM, see {@code calculateNeededAcusFromByteSize}. */
  private static final double GB_PER_ACU = 2;
  private static final double MAX_ACUS = 70;

  /**
   * Constructed directly rather than via {@code getInstance()}, since the provider lookup behind it needs a
   * {@code Config.instance}, which a plain unit test has no reason to set up.
   */
  private final ResourceAndTimeCalculator calculator = new ResourceAndTimeCalculator();

  /**
   * One thread processes one file at a time, so the demand of a step is the demand of a single file times the
   * number of threads.
   */
  @Test
  void derivesTheDemandFromTheSizeOfASingleFile() {
    //8 files of 1 GB each, so one thread needs the ACUs for 1 GB, which is 0.5
    double acus = calculator.calculateNeededImportAcus(8 * GB, 8, 10);

    assertEquals(10 * (1 / GB_PER_ACU), acus, 0.0001);
  }

  @Test
  void scalesLinearlyWithTheThreadCount() {
    double atTen = calculator.calculateNeededImportAcus(8 * GB, 8, 10);
    double atTwenty = calculator.calculateNeededImportAcus(8 * GB, 8, 20);

    assertEquals(2 * atTen, atTwenty, 0.0001, "Doubling the concurrency should double the estimate, not quadruple it.");
    assertTrue(atTwenty < MAX_ACUS, "This case has to stay below the cap, otherwise it proves nothing.");
  }

  /**
   * The same amount of data spread over more files means smaller files, so less memory per thread.
   */
  @Test
  void scalesWithTheFileSizeRatherThanTheTotalSize() {
    double fewLargeFiles = calculator.calculateNeededImportAcus(8 * GB, 4, 10);
    double manySmallFiles = calculator.calculateNeededImportAcus(8 * GB, 8, 10);

    assertEquals(2 * manySmallFiles, fewLargeFiles, 0.0001);
  }

  @Test
  void isCappedToKeepTheJobExecutable() {
    double acus = calculator.calculateNeededImportAcus(800 * GB, 8, 10);

    assertEquals(MAX_ACUS, acus, 0.0001);
  }

  /**
   * An unknown file count must not divide by zero, it counts as a single file.
   */
  @Test
  void treatsAnUnknownFileCountAsOne() {
    double withoutFileCount = calculator.calculateNeededImportAcus(2 * GB, 0, 3);
    double withOneFile = calculator.calculateNeededImportAcus(2 * GB, 1, 3);

    assertEquals(withOneFile, withoutFileCount, 0.0001);
    assertEquals(3 * (2 / GB_PER_ACU), withoutFileCount, 0.0001);
  }
}
