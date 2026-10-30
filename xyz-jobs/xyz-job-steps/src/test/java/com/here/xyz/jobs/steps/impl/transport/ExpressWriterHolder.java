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

/**
 * Holds the {@link ExpressSpaceWriter} of one test instance.
 *
 * <p>A single instance per test is needed because {@code TestSuite} asks for the writer several times per
 * scenario, while the used write implementation is recorded on the writer. The holder is deliberately not static:
 * JUnit creates one test instance per test method, so no state leaks between scenarios even if suites run
 * concurrently.</p>
 */
final class ExpressWriterHolder {

  private ExpressSpaceWriter writer;

  /**
   * @param composite Whether the tested space is a composite space
   * @param suiteClass The test class. Its simple name becomes the table name, because
   *   {@code TestSuite.writeFeatureForPreparation} derives the table of its own {@code SQLSpaceWriter} the same
   *   way. Keep those class names short: the longest derived identifier is {@code <className>_super_version_seq}
   *   and PostgreSQL truncates identifiers at 63 characters.
   */
  ExpressSpaceWriter get(boolean composite, Class<?> suiteClass) {
    if (writer == null)
      writer = new ExpressSpaceWriter(composite, suiteClass.getSimpleName());
    return writer;
  }
}
