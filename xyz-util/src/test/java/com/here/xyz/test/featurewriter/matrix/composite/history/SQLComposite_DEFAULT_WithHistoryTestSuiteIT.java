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

package com.here.xyz.test.featurewriter.matrix.composite.history;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.test.SpaceWritingTest.Operation.D;
import static com.here.xyz.test.SpaceWritingTest.Operation.H;
import static com.here.xyz.test.SpaceWritingTest.Operation.I;
import static com.here.xyz.test.SpaceWritingTest.Operation.J;
import static com.here.xyz.test.SpaceWritingTest.Operation.U;
import static com.here.xyz.test.SpaceWritingTest.SQLError.FEATURE_EXISTS;
import static com.here.xyz.test.SpaceWritingTest.SQLError.FEATURE_NOT_EXISTS;
import static com.here.xyz.test.SpaceWritingTest.SQLError.MERGE_CONFLICT_ERROR;
import static com.here.xyz.test.SpaceWritingTest.SQLError.VERSION_CONFLICT_ERROR;
import static com.here.xyz.test.featurewriter.TestSuite.TableOperation.INSERT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.test.SpaceWritingTest.OnExists;
import com.here.xyz.test.SpaceWritingTest.OnMergeConflict;
import com.here.xyz.test.SpaceWritingTest.OnNotExists;
import com.here.xyz.test.SpaceWritingTest.OnVersionConflict;
import com.here.xyz.test.featurewriter.matrix.noncomposite.history.SQLNonCompositeWithHistoryTestSuiteIT;
import java.util.Collection;
import java.util.List;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SQLComposite_DEFAULT_WithHistoryTestSuiteIT extends SQLNonCompositeWithHistoryTestSuiteIT {

  public SQLComposite_DEFAULT_WithHistoryTestSuiteIT(TestArgs args) {
    super(args.withContext(DEFAULT));
  }

  @Parameters(name = "{0}")
  public static Collection<Object[]> parameterSets() throws JsonProcessingException {
    return testScenarios().stream().map(args -> new Object[]{args}).toList();
  }

  public static List<TestArgs> testScenarios() {
    return List.of(

        new TestArgs("1.1", true, true, false, true, null, null, null, UserIntent.WRITE, OnNotExists.CREATE, null, null, null, DEFAULT,
            null,
            new TestAssertions(INSERT, I)),

        new TestArgs("1.2", true, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.ERROR, null, null, null, DEFAULT,
            null,
            new TestAssertions(FEATURE_NOT_EXISTS)),

        new TestArgs("1.3", true, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.RETAIN, null, null, null, DEFAULT,
            null,
            new TestAssertions()),

        new TestArgs("2.1", true, true, true, true, null, true, false, UserIntent.WRITE, null, OnExists.DELETE, null, null, DEFAULT,
            null,
            new TestAssertions(INSERT, H)),

        new TestArgs("2.2", true, true, true, true, null, true, false, UserIntent.WRITE, null, OnExists.REPLACE, null, null, DEFAULT,
            null,
            new TestAssertions(INSERT, I)),

        new TestArgs("2.3", true, true, true, true, null, true, false, UserIntent.WRITE, null, OnExists.RETAIN, null, null, DEFAULT,
            null,
            new TestAssertions()),

        new TestArgs("2.4", true, true, true, true, null, true, false, UserIntent.WRITE, null, OnExists.ERROR, null, null, DEFAULT,
            null,
            new TestAssertions(FEATURE_EXISTS)),

        new TestArgs("3.1", true, true, true, true, null, false, true, UserIntent.WRITE, null, OnExists.REPLACE, null, null, DEFAULT,
            null,
            new TestAssertions(INSERT, U)),

        new TestArgs("3.2", true, true, true, true, null, false, true, UserIntent.WRITE, null, OnExists.RETAIN, null, null, DEFAULT,
            null,
            new TestAssertions()),

        new TestArgs("3.3", true, true, true, true, null, false, true, UserIntent.WRITE, null, OnExists.ERROR, null, null, DEFAULT,
            null,
            new TestAssertions(FEATURE_EXISTS)),

        new TestArgs("4.1", true, true, true, true, null, true, true, UserIntent.WRITE, null, OnExists.DELETE, null, null, DEFAULT,
            null,
            new TestAssertions(INSERT, J)),

        new TestArgs("4.2", true, true, true, true, null, false, true, UserIntent.WRITE, null, OnExists.DELETE, null, null, DEFAULT,
            null,
            new TestAssertions(INSERT, D)),

        new TestArgs("5.1", true, true, true, false, false, false, true, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.ERROR, null, DEFAULT,
            null,
            new TestAssertions(VERSION_CONFLICT_ERROR)),

        new TestArgs("5.2", true, true, true, false, false, false, true, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.RETAIN, null, DEFAULT,
            null,
            new TestAssertions()),

        new TestArgs("6.1", true, true, true, false, false, true, true, UserIntent.WRITE, null, OnExists.DELETE, OnVersionConflict.REPLACE, null, DEFAULT,
            null,
            new TestAssertions(INSERT, J)),

        new TestArgs("6.2", true, true, true, false, false, false, true, UserIntent.WRITE, null, OnExists.DELETE, OnVersionConflict.REPLACE, null, DEFAULT,
            null,
            new TestAssertions(INSERT, D)),

        new TestArgs("6.3", true, true, true, false, false, false, true, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.REPLACE, null, DEFAULT,
            null,
            new TestAssertions(INSERT, U)),

        new TestArgs("6.4", true, true, true, false, false, false, true, UserIntent.WRITE, null, OnExists.RETAIN, OnVersionConflict.REPLACE, null, DEFAULT,
            null,
            new TestAssertions()),

        new TestArgs("6.5", true, true, true, false, false, false, true, UserIntent.WRITE, null, OnExists.ERROR, OnVersionConflict.REPLACE, null, DEFAULT,
            null,
            new TestAssertions(FEATURE_EXISTS)),

        new TestArgs("7.1", true, true, true, false, false, true, false, UserIntent.WRITE, null, OnExists.DELETE, OnVersionConflict.REPLACE, null, DEFAULT,
            null,
            new TestAssertions(INSERT, H)),

        new TestArgs("7.2", true, true, true, false, false, true, false, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.REPLACE, null, DEFAULT,
            null,
            new TestAssertions(INSERT, I)),

        new TestArgs("7.3", true, true, true, false, false, true, false, UserIntent.WRITE, null, OnExists.RETAIN, OnVersionConflict.REPLACE, null, DEFAULT,
            null,
            new TestAssertions()),

        new TestArgs("7.4", true, true, true, false, false, true, false, UserIntent.WRITE, null, OnExists.ERROR, OnVersionConflict.REPLACE, null, DEFAULT,
            null,
            new TestAssertions(FEATURE_EXISTS)),

        new TestArgs("8.1", true, true, true, false, false, false, true, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.MERGE, null, DEFAULT,
            null,
            new TestAssertions(INSERT, U, true)),

        new TestArgs("9.1", true, true, true, false, true, false, true, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.MERGE, OnMergeConflict.ERROR, DEFAULT,
            null,
            new TestAssertions(MERGE_CONFLICT_ERROR)),

        new TestArgs("9.2", true, true, true, false, true, false, true, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.MERGE, OnMergeConflict.RETAIN, DEFAULT,
            null,
            new TestAssertions()),

        new TestArgs("9.3", true, true, true, false, true, false, true, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.MERGE, OnMergeConflict.REPLACE, DEFAULT,
            null,
            new TestAssertions(INSERT, U, false))

    );
  }
}
