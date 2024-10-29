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

package com.here.xyz.test.featurewriter.sql.composite.history;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.test.featurewriter.SpaceWriter.Operation.D;
import static com.here.xyz.test.featurewriter.SpaceWriter.Operation.H;
import static com.here.xyz.test.featurewriter.SpaceWriter.Operation.I;
import static com.here.xyz.test.featurewriter.SpaceWriter.Operation.J;
import static com.here.xyz.test.featurewriter.SpaceWriter.Operation.U;
import static com.here.xyz.test.featurewriter.TestSuite.TableOperation.INSERT;
import static com.here.xyz.util.db.pg.SQLError.FEATURE_EXISTS;
import static com.here.xyz.util.db.pg.SQLError.FEATURE_NOT_EXISTS;
import static com.here.xyz.util.db.pg.SQLError.MERGE_CONFLICT_ERROR;
import static com.here.xyz.util.db.pg.SQLError.VERSION_CONFLICT_ERROR;

import com.here.xyz.events.UpdateStrategy.OnExists;
import com.here.xyz.events.UpdateStrategy.OnNotExists;
import com.here.xyz.events.UpdateStrategy.OnVersionConflict;
import com.here.xyz.events.UpdateStrategy.OnMergeConflict;
import com.here.xyz.test.featurewriter.sql.noncomposite.history.SQLNonCompositeWithHistoryTestSuiteIT;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class SQLComposite_DEFAULT_WithHistoryTestSuiteIT extends SQLNonCompositeWithHistoryTestSuiteIT {

  public static Stream<TestArgs> testScenarios() {
    return Stream.of(

        new TestArgs("1.1", true, true, UserIntent.WRITE, OnNotExists.CREATE, null, null, null,
            new TestAssertions(INSERT, I)),

        new TestArgs("1.2", true, true, UserIntent.WRITE, OnNotExists.ERROR, null, null, null,
            new TestAssertions(FEATURE_NOT_EXISTS)),

        new TestArgs("1.3", true, true, UserIntent.WRITE, OnNotExists.RETAIN, null, null, null,
            new TestAssertions()),

        new TestArgs("2.1", true, true, true, true, true, false, UserIntent.WRITE, null, OnExists.DELETE, null, null, DEFAULT,
            new TestAssertions(INSERT, H)),

        new TestArgs("2.2", true, true, true, true, true, false, UserIntent.WRITE, null, OnExists.REPLACE, null, null, DEFAULT,
            new TestAssertions(INSERT, I)),

        new TestArgs("2.3", true, true, true, true, true, false, UserIntent.WRITE, null, OnExists.RETAIN, null, null, DEFAULT,
            new TestAssertions()),

        new TestArgs("2.4", true, true, true, true, true, false, UserIntent.WRITE, null, OnExists.ERROR, null, null, DEFAULT,
            new TestAssertions(FEATURE_EXISTS)),

        new TestArgs("3.1", true, true, true, true, false, true, UserIntent.WRITE, null, OnExists.REPLACE, null, null, DEFAULT,
            new TestAssertions(INSERT, U)),

        new TestArgs("3.2", true, true, true, true, false, true, UserIntent.WRITE, null, OnExists.RETAIN, null, null, DEFAULT,
            new TestAssertions()),

        new TestArgs("3.3", true, true, true, true, false, true, UserIntent.WRITE, null, OnExists.ERROR, null, null, DEFAULT,
            new TestAssertions(FEATURE_EXISTS)),

        new TestArgs("4.1", true, true, true, true, true, true, UserIntent.WRITE, null, OnExists.DELETE, null, null, DEFAULT,
            new TestAssertions(INSERT, J)),

        new TestArgs("4.2", true, true, true, true, false, true, UserIntent.WRITE, null, OnExists.DELETE, null, null, DEFAULT,
            new TestAssertions(INSERT, D)),

        new TestArgs("5.1", true, true, true, false, false, false, true, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.ERROR, null, DEFAULT,
            new TestAssertions(VERSION_CONFLICT_ERROR)),

        new TestArgs("5.2", true, true, true, false, false, false, true, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.RETAIN, null, DEFAULT,
            new TestAssertions()),

        new TestArgs("6.1", true, true, true, false, false, true, true, UserIntent.WRITE, null, OnExists.DELETE, OnVersionConflict.REPLACE, null, DEFAULT,
            new TestAssertions(INSERT, J)),

        new TestArgs("6.2", true, true, true, false, false, false, true, UserIntent.WRITE, null, OnExists.DELETE, OnVersionConflict.REPLACE, null, DEFAULT,
            new TestAssertions(INSERT, D)),

        new TestArgs("6.3", true, true, true, false, false, false, true, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.REPLACE, null, DEFAULT,
            new TestAssertions(INSERT, U)),

        new TestArgs("6.4", true, true, true, false, false, false, true, UserIntent.WRITE, null, OnExists.RETAIN, OnVersionConflict.REPLACE, null, DEFAULT,
            new TestAssertions()),

        new TestArgs("6.5", true, true, true, false, false, false, true, UserIntent.WRITE, null, OnExists.ERROR, OnVersionConflict.REPLACE, null, DEFAULT,
            new TestAssertions(FEATURE_EXISTS)),

        new TestArgs("7.1", true, true, true, false, false, true, false, UserIntent.WRITE, null, OnExists.DELETE, OnVersionConflict.REPLACE, null, DEFAULT,
            new TestAssertions(INSERT, H)),

        new TestArgs("7.2", true, true, true, false, false, true, false, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.REPLACE, null, DEFAULT,
            new TestAssertions(INSERT, I)),

        new TestArgs("7.3", true, true, true, false, false, true, false, UserIntent.WRITE, null, OnExists.RETAIN, OnVersionConflict.REPLACE, null, DEFAULT,
            new TestAssertions()),

        new TestArgs("7.4", true, true, true, false, false, true, false, UserIntent.WRITE, null, OnExists.ERROR, OnVersionConflict.REPLACE, null, DEFAULT,
            new TestAssertions(FEATURE_EXISTS)),

        new TestArgs("8.1", true, true, true, false, false, false, true, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.MERGE, null, DEFAULT,
            new TestAssertions(INSERT, U, true)),

        new TestArgs("9.1", true, true, true, false, true, false, true, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.MERGE, OnMergeConflict.ERROR, DEFAULT,
            new TestAssertions(MERGE_CONFLICT_ERROR)),

        new TestArgs("9.2", true, true, true, false, true, false, true, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.MERGE, OnMergeConflict.RETAIN, DEFAULT,
            new TestAssertions()),

        new TestArgs("9.3", true, true, true, false, true, false, true, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.MERGE, OnMergeConflict.REPLACE, DEFAULT,
            new TestAssertions(INSERT, U, false))

    );
  }

  @ParameterizedTest
  @MethodSource("testScenarios")
  void start(TestArgs args) throws Exception {
    runTest(args);
  }

  @Override
  protected TestArgs modifyArgs(TestArgs args) {
    return args.withComposite(true).withContext(DEFAULT);
  }
}
