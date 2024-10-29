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

package com.here.xyz.test.featurewriter.sql.composite.nohistory;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.test.featurewriter.SpaceWriter.Operation.H;
import static com.here.xyz.test.featurewriter.SpaceWriter.Operation.I;
import static com.here.xyz.test.featurewriter.SpaceWriter.Operation.J;
import static com.here.xyz.test.featurewriter.SpaceWriter.Operation.U;
import static com.here.xyz.test.featurewriter.TestSuite.TableOperation.DELETE;
import static com.here.xyz.test.featurewriter.TestSuite.TableOperation.INSERT;
import static com.here.xyz.test.featurewriter.TestSuite.TableOperation.UPDATE;
import static com.here.xyz.util.db.pg.SQLError.FEATURE_EXISTS;
import static com.here.xyz.util.db.pg.SQLError.FEATURE_NOT_EXISTS;
import static com.here.xyz.util.db.pg.SQLError.ILLEGAL_ARGUMENT;
import static com.here.xyz.util.db.pg.SQLError.VERSION_CONFLICT_ERROR;

import com.here.xyz.events.UpdateStrategy.OnExists;
import com.here.xyz.events.UpdateStrategy.OnNotExists;
import com.here.xyz.events.UpdateStrategy.OnVersionConflict;
import com.here.xyz.test.featurewriter.sql.noncomposite.nohistory.SQLNonCompositeNoHistoryTestSuiteIT;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class SQLComposite_DEFAULT_NoHistoryTestSuiteIT extends SQLNonCompositeNoHistoryTestSuiteIT {

  public static Stream<TestArgs> testScenarios() {
    return Stream.of(
        new TestArgs("1.1", true, false, UserIntent.WRITE, OnNotExists.CREATE, null, null, null,
            new TestAssertions(INSERT, I)),

        new TestArgs("1.2", true, false, UserIntent.WRITE, OnNotExists.ERROR, null, null, null,
            new TestAssertions(FEATURE_NOT_EXISTS)),

        new TestArgs("1.3", true, false, UserIntent.WRITE, OnNotExists.RETAIN, null, null, null,
            new TestAssertions()),

        new TestArgs("2.1", true, false, true, true, true, false, UserIntent.WRITE, null, OnExists.DELETE, null, null, DEFAULT,
            new TestAssertions(INSERT, H)),

        new TestArgs("2.2", true, false, true, true, true, false, UserIntent.WRITE, null, OnExists.REPLACE, null, null, DEFAULT,
            new TestAssertions(INSERT, I)),

        new TestArgs("2.3", true, false, true, true, true, false, UserIntent.WRITE, null, OnExists.RETAIN, null, null, DEFAULT,
            new TestAssertions()),

        new TestArgs("2.4", true, false, true, true, true, false, UserIntent.WRITE, null, OnExists.ERROR, null, null, DEFAULT,
            new TestAssertions(FEATURE_EXISTS)),

        new TestArgs("3.1", true, false, true, true, false, true, UserIntent.WRITE, null, OnExists.REPLACE, null, null, DEFAULT,
            new TestAssertions(UPDATE, U)),

        new TestArgs("3.2", true, false, true, true, false, true, UserIntent.WRITE, null, OnExists.RETAIN, null, null, DEFAULT,
            new TestAssertions()),

        new TestArgs("3.3", true, false, true, true, false, true, UserIntent.WRITE, null, OnExists.ERROR, null, null, DEFAULT,
            new TestAssertions(FEATURE_EXISTS)),

        new TestArgs("4.1", true, false, true, true, true, true, UserIntent.WRITE, null, OnExists.DELETE, null, null, DEFAULT,
            new TestAssertions(UPDATE, J)),

        new TestArgs("4.2", true, false, true, true, false, true, UserIntent.WRITE, null, OnExists.DELETE, null, null, DEFAULT,
            new TestAssertions(DELETE)),

        new TestArgs("5.1", true, false, true, false, false, false, true, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.ERROR, null, DEFAULT,
            new TestAssertions(VERSION_CONFLICT_ERROR)),

        new TestArgs("5.2", true, false, true, false, false, false, true, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.RETAIN, null, DEFAULT,
            new TestAssertions()),

        new TestArgs("5.3", true, false, true, false, false, false, true, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.REPLACE, null, DEFAULT,
            new TestAssertions(UPDATE, U)),

        new TestArgs("5.4", true, false, true, false, false, true, false, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.REPLACE, null, DEFAULT,
            new TestAssertions(INSERT, I)),

        new TestArgs("5.5", true, false, true, false, false, false, true, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.MERGE, null, DEFAULT,
            new TestAssertions(ILLEGAL_ARGUMENT))

    );
  }

  @Override
  protected TestArgs modifyArgs(TestArgs args) {
    return args.withComposite(true).withContext(DEFAULT);
  }

  @ParameterizedTest
  @MethodSource("testScenarios")
  void start(TestArgs args) throws Exception {
    runTest(args);
  }
}
