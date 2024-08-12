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

package com.here.xyz.test.featurewriter.noncomposite.history;

import static com.here.xyz.test.GenericSpaceBased.Operation.U;
import static com.here.xyz.test.GenericSpaceBased.SQLError.FEATURE_EXISTS;
import static com.here.xyz.test.featurewriter.TestSuite.TableOperation.INSERT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.test.GenericSpaceBased.OnExists;
import com.here.xyz.test.GenericSpaceBased.OnMergeConflict;
import com.here.xyz.test.GenericSpaceBased.OnNotExists;
import com.here.xyz.test.GenericSpaceBased.OnVersionConflict;
import com.here.xyz.test.GenericSpaceBased.Operation;
import com.here.xyz.test.GenericSpaceBased.SQLError;
import com.here.xyz.test.featurewriter.HubBasedTestSuite;
import java.util.Collection;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class HubNonCompositeWithHistoryTestSuiteIT extends HubBasedTestSuite {

  public HubNonCompositeWithHistoryTestSuiteIT(TestArgs args) {
    super(args);
  }

  @Parameters(name = "{0}")
  public static Collection<Object[]> parameterSets() throws JsonProcessingException {
    return testScenarios().stream().map(args -> new Object[]{args}).toList();
  }

  public static Collection<TestArgs> testScenarios() throws JsonProcessingException {
    return List.of(
        /** Feature NOT exists */
        new TestArgs("0", false, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.CREATE, null, null, null, null,
            /* Expected content of the written Feature */
            new Expectations(
                INSERT,  //expectedTableOperation
                Operation.I,  //expectedFeatureOperation
                simpleFeature(),  //expectedFeature
                1L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                null  //expectedSQLError
            )
        ),
        /* No existing Feature expected. No TableOperation! */
        new TestArgs("1", false, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.ERROR, null, null, null, null,
            new Expectations(SQLError.FEATURE_NOT_EXISTS)),
        /* No existing Feature expected. No TableOperation! */
        new TestArgs("2", false, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.RETAIN, null, null, null, null, null),

        /** Feature exists + no ConflictHandling */
        new TestArgs("3", false, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, null, null, null,
            /* Expected content of the deleted Feature (History). */
            new Expectations(
                INSERT,  //expectedTableOperation
                Operation.D,  //expectedFeatureOperation
                simple1stModifiedFeature(),  //expectedFeature
                2L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                null  //expectedSQLError
            )
        ),
        new TestArgs("4", false, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, null,
            /* Expected content of the replaced Feature (2th write). */
            new Expectations(
                INSERT,  //expectedTableOperation
                U,  //expectedFeatureOperation
                simple1stModifiedFeature(),  //expectedFeature
                2L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                null  //expectedSQLError
            )
        ),
        new TestArgs("5", false, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, null, null, null,
            /* Expected content of the untouched Feature (from 1th write). No TableOperation! */
            new Expectations(
                INSERT,  //expectedTableOperation
                Operation.I,  //expectedFeatureOperation
                simpleFeature(),  //expectedFeature
                1L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                null  //expectedSQLError
            )
        ),
        new TestArgs("6", false, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, null, null, null,
            /* SQLError.FEATURE_EXISTS & Expected content of the untouched Feature (from 1th write). No TableOperation! */
            new Expectations(
                INSERT,  //expectedTableOperation
                Operation.I,  //expectedFeatureOperation
                simpleFeature(),  //expectedFeature
                1L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                FEATURE_EXISTS  //expectedSQLError
            )
        ),
        /** Feature exists and got updated. Third write will have a Baseversion MATCH */
        new TestArgs("7", false, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, OnVersionConflict.REPLACE,
            null, null,
            /* Expected content of the deleted Feature (3rd write) (History) */
            new Expectations(
                INSERT,  //expectedTableOperation
                Operation.D,  //expectedFeatureOperation
                simple1stModifiedFeature(),  //expectedFeature
                //TODO: Check if version=3 is correct!
                3L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                null  //expectedSQLError
            )
        ),
        new TestArgs("8", false, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.REPLACE,
            null, null,
            /* Expected content of the replaced Feature (3th write) */
            new Expectations(
                INSERT,  //expectedTableOperation
                U,  //expectedFeatureOperation
                simple2ndModifiedFeature(3L, false),  //expectedFeature
                3L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                null  //expectedSQLError
            )
        ),
        new TestArgs("9", false, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, OnVersionConflict.REPLACE,
            null, null,
            /* Expected content of the untouched Feature (from 2th write). No TableOperation! */
            new Expectations(
                INSERT,  //expectedTableOperation
                U,  //expectedFeatureOperation
                simple1stModifiedFeature(),  //expectedFeature
                2L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                null  //expectedSQLError
            )
        ),
        new TestArgs("10", false, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, OnVersionConflict.REPLACE,
            null, null,
            /* SQLError.FEATURE_EXISTS & Expected content of the untouched Feature (from 2th write). No TableOperation!  */
            new Expectations(
                INSERT,  //expectedTableOperation
                U,  //expectedFeatureOperation
                simple1stModifiedFeature(),  //expectedFeature
                2L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                FEATURE_EXISTS  //expectedSQLError
            )
        ),

        /** Feature exists and got updated. Third write will have a Baseversion MISSMATCH. With ConflictHandling -> ERROR, RETAIN  */
        new TestArgs("11", false, true, true, false, null, null, null, UserIntent.WRITE, null, null, OnVersionConflict.ERROR, null, null,
            /* SQLError.VERSION_CONFLICT_ERROR & Expected content of the untouched Feature (from 2th write). */
            new Expectations(
                INSERT,  //expectedTableOperation
                U,  //expectedFeatureOperation
                simple1stModifiedFeature(),  //expectedFeature
                2L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                SQLError.VERSION_CONFLICT_ERROR  //expectedSQLError
            )
        ),
        new TestArgs("12", false, true, true, false, null, null, null, UserIntent.WRITE, null, null, OnVersionConflict.RETAIN, null, null,
            /* Expected content of the untouched Feature (from 2th write). No TableOperation! */
            new Expectations(
                INSERT,  //expectedTableOperation
                U,  //expectedFeatureOperation
                simple1stModifiedFeature(),  //expectedFeature
                2L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                null  //expectedSQLError
            )
        ),
        /**  Feature exists and got updated. Third write will have a Baseversion MISSMATCH. With ConflictHandling -> REPLACE */
        new TestArgs("13", false, true, true, false, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, OnVersionConflict.REPLACE,
            null, null,
            /* Expected content of the deleted Feature (3rd write) (History) */
            new Expectations(
                INSERT,  //expectedTableOperation
                Operation.D,  //expectedFeatureOperation
                simple1stModifiedFeature(),  //expectedFeature
                3L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                null  //expectedSQLError
            )
        ),
        new TestArgs("14", false, true, true, false, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.REPLACE,
            null, null,
            /* Expected content of the replaced Feature (3th write). */
            new Expectations(
                INSERT,  //expectedTableOperation
                U,  //expectedFeatureOperation
                simple2ndModifiedFeature(3L, false),  //expectedFeature
                3L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                null  //expectedSQLError
            )
        ),
        new TestArgs("15", false, true, true, false, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, OnVersionConflict.REPLACE,
            null, null,
            /* Expected content of the untouched Feature (from 2th write). No TableOperation! */
            new Expectations(
                INSERT,  //expectedTableOperation
                U,  //expectedFeatureOperation
                simple1stModifiedFeature(),  //expectedFeature
                2L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                null  //expectedSQLError
            )
        ),
        new TestArgs("16", false, true, true, false, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, OnVersionConflict.REPLACE,
            null, null,
            /* Expected content of the untouched Feature (from 2th write). No TableOperation! */
            new Expectations(
                INSERT,  //expectedTableOperation
                U,  //expectedFeatureOperation
                simple1stModifiedFeature(),  //expectedFeature
                2L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                FEATURE_EXISTS  //expectedSQLError
            )
        ),
        /** Feature exists and got updated. Third write will have a Baseversion MISSMATCH. With ConflictHandling -> MERGE (NoConflicting Changes) */
        new TestArgs("17", false, true, true, false, false, null, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, null, null,
            /* Expected content of the merged Feature (from 2th&3th write). */
            new Expectations(
                INSERT, //expectedTableOperation
                //TODO: Check
                U, //expectedFeatureOperation
                simple1stModifiedFeature().withProperties(simple1stModifiedFeature().getProperties().with("age", "32")), //expectedFeature
                3L, //expectedVersion
                Long.MAX_VALUE, //expectedNextVersion
                null, //expectedAuthor
                null //expectedSQLError
            )
        ),

        /** Feature exists + With ConflictHandling + Baseversion MISSMATCH -> MERGE Conflicting*/
        //TODO: Test is flickering! Assumption: hub behavior is not consistent.
        new TestArgs("18", false, true, true, false, true, null, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE,
            OnMergeConflict.ERROR,
            null,
            /* Expected content of the untouched Feature (from 2th write). No TableOperation! */
            new Expectations(
                INSERT,  //expectedTableOperation
                U,  //expectedFeatureOperation
                simple1stModifiedFeature(),  //expectedFeature
                2L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                SQLError.MERGE_CONFLICT_ERROR  //expectedSQLError
            )
        ),
        //TODO: Test is flickering! Assumption: hub behavior is not consistent.
        new TestArgs("19", false, true, true, false, true, null, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE,
            OnMergeConflict.RETAIN,
            null,
            /* Expected content of the untouched Feature (from 2th write). No TableOperation! */
            new Expectations(
                INSERT,  //expectedTableOperation
                U,  //expectedFeatureOperation
                simple1stModifiedFeature(),  //expectedFeature
                2L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                null  //expectedSQLError
            )
        ),
        //TODO: Test is flickering! Assumption: hub behavior is not consistent.
        new TestArgs("20", false, true, true, false, true, null, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE,
            OnMergeConflict.REPLACE,
            null,
            /* Expected content of the replaced Feature (3th write). */
            new Expectations(
                INSERT,  //expectedTableOperation
                U,  //expectedFeatureOperation
                simple2ndModifiedFeature(3L, true),  //expectedFeature
                3L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                null  //expectedSQLError
            )
        )
    );
  }

  @Test
  public void start() throws Exception {
    featureWriterExecutor();
  }
}
