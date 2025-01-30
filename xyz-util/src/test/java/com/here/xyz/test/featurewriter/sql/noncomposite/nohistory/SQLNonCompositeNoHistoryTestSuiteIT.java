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

package com.here.xyz.test.featurewriter.sql.noncomposite.nohistory;

import static com.here.xyz.test.featurewriter.SpaceWriter.Operation.I;
import static com.here.xyz.test.featurewriter.SpaceWriter.Operation.U;
import static com.here.xyz.test.featurewriter.TestSuite.TableOperation.DELETE;
import static com.here.xyz.test.featurewriter.TestSuite.TableOperation.INSERT;
import static com.here.xyz.test.featurewriter.TestSuite.TableOperation.UPDATE;
import static com.here.xyz.util.db.pg.SQLError.FEATURE_EXISTS;
import static com.here.xyz.util.db.pg.SQLError.FEATURE_NOT_EXISTS;
import static com.here.xyz.util.db.pg.SQLError.ILLEGAL_ARGUMENT;
import static com.here.xyz.util.db.pg.SQLError.VERSION_CONFLICT_ERROR;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.events.UpdateStrategy.OnExists;
import com.here.xyz.events.UpdateStrategy.OnNotExists;
import com.here.xyz.events.UpdateStrategy.OnVersionConflict;
import com.here.xyz.test.featurewriter.sql.SQLTestSuite;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class SQLNonCompositeNoHistoryTestSuiteIT extends SQLTestSuite {

  public static Stream<TestArgs> testScenarios() throws JsonProcessingException {
    return Stream.of(
        /** Feature not exists */
        new TestArgs("1", false, false, false, true,  null, null, UserIntent.WRITE, OnNotExists.CREATE, null, null, null, null,
            /* Expected content of newly created Feature */
            new TestAssertions(INSERT, I)
        ),
        /* No existing Feature expected. No TableOperation!  */
        new TestArgs("2", false, false, UserIntent.WRITE, OnNotExists.ERROR, null, null, null, null,
            new TestAssertions(FEATURE_NOT_EXISTS)),
        /* No existing Feature expected. No TableOperation!  */
        new TestArgs("3", false, false, UserIntent.WRITE, OnNotExists.RETAIN, null, null, null, null,
            new TestAssertions()),

        /** Feature exists */
        /* No existing Feature expected */
        new TestArgs("4", false, true, UserIntent.WRITE, null, OnExists.DELETE, null, null, null,
            new TestAssertions(DELETE)),
        new TestArgs("5", false, true, UserIntent.WRITE, null, OnExists.REPLACE, null, null, null,
            /* Expected content of the replaced Feature */
            new TestAssertions(UPDATE, U)
        ),
        new TestArgs("6", false, true, UserIntent.WRITE, null, OnExists.RETAIN, null, null, null,
            /* Expected content of first written Feature which should stay untouched. No TableOperation! */
            new TestAssertions()
        ),
        new TestArgs("7", false, true, UserIntent.WRITE, null, OnExists.ERROR, null, null, null,
            /* SQLError.FEATURE_EXISTS & Expected content of first written Feature which should stay untouched. No TableOperation! */
            new TestAssertions(FEATURE_EXISTS)
        ),

        /** Feature exists and got updated. Third write will have a Baseversion MATCH */
        /* No existing Feature expected */
        new TestArgs("8", false, true, true, false, UserIntent.WRITE, null, OnExists.DELETE, OnVersionConflict.REPLACE, null, null,
            new TestAssertions(DELETE)),
        new TestArgs("9", false, true, true, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.REPLACE, null, null,
            /* Expected content of the replaced Feature (3th write). */
            new TestAssertions(UPDATE, U)
        ),
        new TestArgs("10", false, true, true, UserIntent.WRITE, null, OnExists.RETAIN, OnVersionConflict.REPLACE,
            null, null,
            /* Expected content of the untouched Feature (from 2th write). No TableOperation! */
            new TestAssertions()
        ),
        new TestArgs("11", false, true, true, UserIntent.WRITE, null, OnExists.ERROR, OnVersionConflict.REPLACE,
            null, null,
            /* SQLError.FEATURE_EXISTS & Expected content of the untouched Feature (from 2th write). No TableOperation! */
            new TestAssertions(FEATURE_EXISTS)
        ),
        /** Feature exists and got updated. Third write will have a base version MISMATCH */
        new TestArgs("12", false, true, false, UserIntent.WRITE, null, null, OnVersionConflict.ERROR, null, null,
            /* SQLError.VERSION_CONFLICT_ERROR & Expected content of the untouched Feature (from 2th write). No TableOperation! */
            new TestAssertions(VERSION_CONFLICT_ERROR)
        ),
        new TestArgs("13", false, true, false, UserIntent.WRITE, null, null, OnVersionConflict.RETAIN, null, null,
            /* Expected content of the untouched Feature (from 2th write). No TableOperation! */
            new TestAssertions()
        ),
        new TestArgs("14", false, true, false, UserIntent.WRITE, null, null, OnVersionConflict.REPLACE, null, null,
            /* Expected content of the replaced Feature (from 3th write). */
            new TestAssertions(UPDATE, U)
        ),
        new TestArgs("15", false, true, false, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, null, null,
            /* SQLError.ILLEGAL_ARGUMENT & Expected content of the untouched Feature (from 2th write). No TableOperation!  */
            new TestAssertions(ILLEGAL_ARGUMENT)
        )
    );
  }

  @ParameterizedTest
  @MethodSource("testScenarios")
  void start(TestArgs args) throws Exception {
    runTest(args);
  }
}
