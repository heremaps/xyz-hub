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

package com.here.xyz.jobs.steps.impl.transport;

import com.here.xyz.jobs.steps.impl.transport.ExpressSpaceWriter.WriterMode;
import com.here.xyz.test.featurewriter.TestSuite;

/**
 * Binds the scenario runner of {@link TestSuite} to the {@link ExpressSpaceWriter}, the counterpart of
 * {@code com.here.xyz.test.featurewriter.sql.SQLTestSuite} for the express import writer.
 *
 * <p>Subclasses only provide the scenarios. They are expected to reference the scenario streams of the existing
 * FeatureWriter suites through a fully qualified {@code @MethodSource}, so that both write implementations are
 * held against literally the same expectations. Referencing instead of inheriting from those suites is
 * intentional: it keeps the express suites free of the inherited (package-private) test methods of the SQL
 * suites, which would otherwise make it ambiguous how often a scenario is executed.</p>
 */
public abstract class ExpressTestSuite extends TestSuite {

  private final ExpressWriterHolder expressWriter = new ExpressWriterHolder();

  /**
   * {@inheritDoc}
   *
   * <p>Resolving this lazily is safe, because {@code TestSuite.prepare()} calls
   * {@code init(modifyArgs(EMPTY_ARGS))} before the first access, so the composite flag already carries the value
   * of the concrete suite.</p>
   */
  @Override
  protected ExpressSpaceWriter spaceWriter() {
    return expressWriter.get(composite, getClass());
  }

  /**
   * @return The write implementation which performed the write of the current scenario.
   */
  protected WriterMode usedWriterMode() {
    return spaceWriter().lastUsedWriterMode();
  }
}
