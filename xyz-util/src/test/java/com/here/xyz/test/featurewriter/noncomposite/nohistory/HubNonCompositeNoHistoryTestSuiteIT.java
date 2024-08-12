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

package com.here.xyz.test.featurewriter.noncomposite.nohistory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.test.GenericSpaceBased.OnExists;
import com.here.xyz.test.GenericSpaceBased.OnNotExists;
import com.here.xyz.test.GenericSpaceBased.OnVersionConflict;
import com.here.xyz.test.GenericSpaceBased.Operation;
import com.here.xyz.test.GenericSpaceBased.SQLError;
import com.here.xyz.test.featurewriter.HubBasedTestSuite;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class HubNonCompositeNoHistoryTestSuiteIT extends HubBasedTestSuite {

  public HubNonCompositeNoHistoryTestSuiteIT(TestArgs args) {
    super(args);
  }

  @Parameters(name = "{0}")
  public static List<Object[]> parameterSets() throws JsonProcessingException {
    return testScenarios().stream().map(args -> new Object[]{args}).toList();
  }

  public static List<TestArgs> testScenarios() throws JsonProcessingException {
    return List.of(
        /** Feature not exists */
        new TestArgs(
            "0",    //testName
            true,  //composite
            false,  //history
            false,  //featureExists
            null,  //baseVersionMatch
            null,  //conflictingAttributes
            null,  //featureExistsInSuper
            null,  //featureExistsInExtension

            UserIntent.WRITE,  //userIntent
            OnNotExists.CREATE,  //onNotExists
            null,  //onExists
            null,  //onVersionConflict
            null,  //onMergeConflict
            null,  //spaceContext

            /* Expected content of newly created Feature */
            new Expectations(
                TableOperation.INSERT,  //expectedTableOperation
                Operation.I,  //expectedFeatureOperation
                simpleFeature(),  //expectedFeature
                1L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor - Hub is getting used without auth
                null  //expectedSQLError
            )
        ),
        /* No existing Feature expected. No TableOperation!  */
        new TestArgs("2", false, false, false, null, null, null, null, UserIntent.WRITE, OnNotExists.ERROR, null, null, null, null,
            new Expectations(SQLError.FEATURE_NOT_EXISTS)),
        /* No existing Feature expected. No TableOperation!  */
        new TestArgs("3", false, false, false, null, null, null, null, UserIntent.WRITE, OnNotExists.RETAIN, null, null, null, null, null),

        /** Feature exists */
        /* No existing Feature expected */
        new TestArgs("4", false, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, null, null, null,
            new Expectations(TableOperation.DELETE)),
        new TestArgs("5", false, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, null,
            /* Expected content of the replaced Feature */
            new Expectations(
                TableOperation.UPDATE,  //expectedTableOperation
                Operation.U,  //expectedFeatureOperation
                simple1stModifiedFeature(),  //expectedFeature
                2L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                null  //expectedSQLError
            )
        ),
        new TestArgs("6", false, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, null, null, null,
            /* Expected content of first written Feature which should stay untouched. No TableOperation! */
            new Expectations(
                TableOperation.INSERT,  //expectedTableOperation
                Operation.I,  //expectedFeatureOperation
                simple1stModifiedFeature(),  //expectedFeature
                1L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                null  //expectedSQLError
            )
        ),
        new TestArgs("7", false, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, null, null, null,
            /* SQLError.FEATURE_EXISTS & Expected content of first written Feature which should stay untouched. No TableOperation! */
            new Expectations(
                TableOperation.INSERT,  //expectedTableOperation
                Operation.I,  //expectedFeatureOperation
                simpleFeature(),  //expectedFeature
                1L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                SQLError.FEATURE_EXISTS  //expectedSQLError
            )
        ),

        /** Feature exists and got updated. Third write will have a Baseversion MATCH */
        /* No existing Feature expected */
        new TestArgs("8", false, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, OnVersionConflict.REPLACE,
            null, null,
            new Expectations(TableOperation.DELETE)),
        new TestArgs("9", false, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.REPLACE,
            null, null,
            /* Expected content of the replaced Feature (3th write). */
            new Expectations(
                TableOperation.UPDATE,  //expectedTableOperation
                Operation.U,  //expectedFeatureOperation
                simple2ndModifiedFeature(3L, false),  //expectedFeature
                3L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                null  //expectedSQLError
            )
        ),
        new TestArgs("10", false, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, OnVersionConflict.REPLACE,
            null, null,
            /* Expected content of the untouched Feature (from 2th write). No TableOperation! */
            new Expectations(
                TableOperation.INSERT,  //expectedTableOperation
                Operation.U,  //expectedFeatureOperation
                simple1stModifiedFeature(),  //expectedFeature
                2L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                null  //expectedSQLError
            )
        ),
        new TestArgs("11", false, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, OnVersionConflict.REPLACE,
            null, null,
            /* SQLError.FEATURE_EXISTS & Expected content of the untouched Feature (from 2th write). No TableOperation! */
            new Expectations(
                TableOperation.INSERT,  //expectedTableOperation
                Operation.U,  //expectedFeatureOperation
                simple1stModifiedFeature(),  //expectedFeature
                2L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                SQLError.FEATURE_EXISTS  //expectedSQLError
            )
        ),
        /** Feature exists and got updated. Third write will have a Baseversion MISSMATCH */
        new TestArgs("12", false, false, true, false, null, null, null, UserIntent.WRITE, null, null, OnVersionConflict.ERROR, null, null,
            /* SQLError.VERSION_CONFLICT_ERROR & Expected content of the untouched Feature (from 2th write). No TableOperation! */
            new Expectations(
                TableOperation.INSERT,  //expectedTableOperation
                Operation.U,  //expectedFeatureOperation
                simple1stModifiedFeature(),  //expectedFeature
                2L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                SQLError.VERSION_CONFLICT_ERROR  //expectedSQLError
            )
        ),
        new TestArgs("13", false, false, true, false, null, null, null, UserIntent.WRITE, null, null, OnVersionConflict.RETAIN, null, null,
            /* Expected content of the untouched Feature (from 2th write). No TableOperation! */
            new Expectations(
                TableOperation.INSERT,  //expectedTableOperation
                Operation.U,  //expectedFeatureOperation
                simple1stModifiedFeature(),  //expectedFeature
                2L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                null  //expectedSQLError
            )
        ),
        new TestArgs("14", false, false, true, false, null, null, null, UserIntent.WRITE, null, null, OnVersionConflict.REPLACE, null, null,
            /* Expected content of the replaced Feature (from 3th write). */
            new Expectations(
                TableOperation.UPDATE,  //expectedTableOperation
                Operation.U,  //expectedFeatureOperation
                simple2ndModifiedFeature(3L, false),  //expectedFeature
                3L,  //expectedVersion
                Long.MAX_VALUE,  //expectedNextVersion
                null,  //expectedAuthor
                null  //expectedSQLError
            )
        )
        //TODO: Hub does not raise an error
//        new TestArgs( "15", false, false, true, false, null, null, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, null, null,
//                /* SQLError.ILLEGAL_ARGUMEN & Expected content of the untouched Feature (from 2th write). No TableOperation!  */
//                new Expectations(
//                    TableOperation.INSERT,  //expectedTableOperation
//                    Operation.U,  //expectedFeatureOperation
//                    simple1thModificatedFeature(),  //expectedFeature
//                    2L,  //expectedVersion
//                    Long.MAX_VALUE,  //expectedNextVersion
//                    null,  //expectedAuthor
//                    SQLError.ILLEGAL_ARGUMENT  //expectedSQLError
//                ),
//            ),
    );
  }

  @Test
  public void start() throws Exception {
    featureWriterExecutor();
  }
}
