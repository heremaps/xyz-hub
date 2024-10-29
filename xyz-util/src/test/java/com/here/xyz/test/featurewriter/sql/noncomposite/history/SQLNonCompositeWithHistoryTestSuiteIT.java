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

package com.here.xyz.test.featurewriter.sql.noncomposite.history;

import static com.here.xyz.test.featurewriter.SpaceWriter.Operation.D;
import static com.here.xyz.test.featurewriter.SpaceWriter.Operation.I;
import static com.here.xyz.test.featurewriter.SpaceWriter.Operation.U;
import static com.here.xyz.test.featurewriter.TestSuite.TableOperation.INSERT;
import static com.here.xyz.util.db.pg.SQLError.FEATURE_EXISTS;
import static com.here.xyz.util.db.pg.SQLError.FEATURE_NOT_EXISTS;
import static com.here.xyz.util.db.pg.SQLError.MERGE_CONFLICT_ERROR;
import static com.here.xyz.util.db.pg.SQLError.VERSION_CONFLICT_ERROR;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.events.UpdateStrategy.OnExists;
import com.here.xyz.events.UpdateStrategy.OnNotExists;
import com.here.xyz.events.UpdateStrategy.OnVersionConflict;
import com.here.xyz.events.UpdateStrategy.OnMergeConflict;
import com.here.xyz.test.featurewriter.sql.SQLTestSuite;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class SQLNonCompositeWithHistoryTestSuiteIT extends SQLTestSuite {

  public static Stream<TestArgs> testScenarios() throws JsonProcessingException {
    return Stream.of(
        /** Feature NOT exists */
        new TestArgs("1", true, false, UserIntent.WRITE, OnNotExists.CREATE, null, null, null, null,
            /* Expected content of the written Feature */
            new TestAssertions(INSERT, I)
        ),
        /* No existing Feature expected. No TableOperation! */
        new TestArgs("2", true, false, UserIntent.WRITE, OnNotExists.ERROR, null, null, null, null,
            new TestAssertions(FEATURE_NOT_EXISTS)),
        /* No existing Feature expected. No TableOperation! */
        new TestArgs("3", true, false, UserIntent.WRITE, OnNotExists.RETAIN, null, null, null, null,
            new TestAssertions()),

        /** Feature exists + no ConflictHandling */
        new TestArgs("4", true, true, UserIntent.WRITE, null, OnExists.DELETE, null, null, null,
            /* Expected content of the deleted Feature (History). */
            new TestAssertions(INSERT, D)
        ),
        new TestArgs("5", true, true, UserIntent.WRITE, null, OnExists.REPLACE, null, null, null,
            /* Expected content of the replaced Feature (2th write). */
            new TestAssertions(INSERT, U)
        ),
        new TestArgs("6", true, true, UserIntent.WRITE, null, OnExists.RETAIN, null, null, null,
            /* Expected content of the untouched Feature (from 1th write). No TableOperation! */
            new TestAssertions()
        ),
        new TestArgs("7", true, true, UserIntent.WRITE, null, OnExists.ERROR, null, null, null,
            /* SQLError.FEATURE_EXISTS & Expected content of the untouched Feature (from 1th write). No TableOperation! */
            new TestAssertions(FEATURE_EXISTS)
        ),
        /** Feature exists and got updated. Third write will have a Baseversion MATCH */
        new TestArgs("8", true, true, true, UserIntent.WRITE, null, OnExists.DELETE, OnVersionConflict.REPLACE,
            null, null,
            /* Expected content of the deleted Feature (3rd write) (History) */
            new TestAssertions(INSERT, D)
        ),
        new TestArgs("9", true, true, true, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.REPLACE,
            null, null,
            /* Expected content of the replaced Feature (3th write) */
            new TestAssertions(INSERT, U)
        ),
        new TestArgs("10", true, true, true, UserIntent.WRITE, null, OnExists.RETAIN, OnVersionConflict.REPLACE,
            null, null,
            /* Expected content of the untouched Feature (from 2th write). No TableOperation! */
            new TestAssertions()
        ),
        new TestArgs("11", true, true, true, UserIntent.WRITE, null, OnExists.ERROR, OnVersionConflict.REPLACE,
            null, null,
            /* SQLError.FEATURE_EXISTS & Expected content of the untouched Feature (from 2th write). No TableOperation!  */
            new TestAssertions(FEATURE_EXISTS)
        ),

        /** Feature exists and got updated. Third write will have a Baseversion MISSMATCH. With ConflictHandling -> ERROR, RETAIN  */
        new TestArgs("12", true, true, false, UserIntent.WRITE, null, null, OnVersionConflict.ERROR, null, null,
            /* SQLError.VERSION_CONFLICT_ERROR & Expected content of the untouched Feature (from 2th write). */
            new TestAssertions(VERSION_CONFLICT_ERROR)
        ),
        new TestArgs("13", true, true, false, UserIntent.WRITE, null, null, OnVersionConflict.RETAIN, null, null,
            /* Expected content of the untouched Feature (from 2th write). No TableOperation! */
            new TestAssertions()
        ),
        /**  Feature exists and got updated. Third write will have a Baseversion MISSMATCH. With ConflictHandling -> REPLACE */
        new TestArgs("14", true, true, false, UserIntent.WRITE, null, OnExists.DELETE, OnVersionConflict.REPLACE,
            null, null,
            /* Expected content of the deleted Feature (3rd write) (History) */
            new TestAssertions(INSERT, D)
        ),
        new TestArgs("15", true, true, false, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.REPLACE,
            null, null,
            /* Expected content of the replaced Feature (3th write). */
            new TestAssertions(INSERT, U)
        ),
        new TestArgs("16", true, true, false, UserIntent.WRITE, null, OnExists.RETAIN, OnVersionConflict.REPLACE,
            null, null,
            /* Expected content of the untouched Feature (from 2th write). No TableOperation! */
            new TestAssertions()
        ),
        new TestArgs("17", true, true, false, UserIntent.WRITE, null, OnExists.ERROR, OnVersionConflict.REPLACE,
            null, null,
            /* Expected content of the untouched Feature (from 2th write). No TableOperation! */
            new TestAssertions(FEATURE_EXISTS)
        ),
        /** Feature exists and got updated. Third write will have a Baseversion MISSMATCH. With ConflictHandling -> MERGE (NoConflicting Changes) */
        new TestArgs("18", true, true, false, false, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, null, null,
            /* Expected content of the merged Feature (from 2th&3th write). */
            new TestAssertions(INSERT, U, true)
        ),

        /** Feature exists + With ConflictHandling + Baseversion MISSMATCH -> MERGE Conflicting*/
        new TestArgs("19", true, true, false, true, UserIntent.WRITE, null, null, OnVersionConflict.MERGE,
            OnMergeConflict.ERROR,
            null,
            /* Expected content of the untouched Feature (from 2th write). No TableOperation! */
            new TestAssertions(MERGE_CONFLICT_ERROR)
        ),
        new TestArgs("20", true, true, false, true, UserIntent.WRITE, null, null, OnVersionConflict.MERGE,
            OnMergeConflict.RETAIN,
            null,
            /* Expected content of the untouched Feature (from 2th write). No TableOperation! */
            new TestAssertions()
        ),
        new TestArgs("21", true, true, false, true, UserIntent.WRITE, null, null, OnVersionConflict.MERGE,
            OnMergeConflict.REPLACE,
            null,
            /* Expected content of the replaced Feature (3th write). */
            new TestAssertions(INSERT, U)
        )
    );
  }

  @ParameterizedTest
  @MethodSource("testScenarios")
  void start(TestArgs args) throws Exception {
    runTest(args);
  }
}
