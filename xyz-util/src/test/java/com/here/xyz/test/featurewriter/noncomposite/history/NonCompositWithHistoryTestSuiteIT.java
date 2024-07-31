package com.here.xyz.test.featurewriter.noncomposite.history;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.test.featurewriter.TestSuiteIT;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class NonCompositWithHistoryTestSuiteIT extends TestSuiteIT {

    public NonCompositWithHistoryTestSuiteIT(String testName, boolean composite, boolean history, boolean featureExists,
                                            Boolean baseVersionMatch, Boolean conflictingAttributes, Boolean featureExistsInSuper, Boolean featureExistsInExtension,
                                            UserIntent userIntent, OnNotExists onNotExists, OnExists onExists, OnVersionConflict onVersionConflict,
                                            OnMergeConflict onMergeConflict, SpaceContext spaceContext, TestSuiteIT.Expectations expectations) {
        super(testName, composite, history, featureExists, baseVersionMatch, conflictingAttributes, featureExistsInSuper, featureExistsInExtension,
                userIntent, onNotExists, onExists, onVersionConflict, onMergeConflict, spaceContext, expectations);
    }

    @Parameterized.Parameters(name = "Test Number #{0}")
    public static Collection<Object[]> testScenarios() throws JsonProcessingException {
        return Arrays.asList(new Object[][]{
            /** Feature NOT exists */
           {  "0", false, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.CREATE, null, null, null, null,
                   new Expectations(
                    TableOperation.INSERT,  //expectedTableOperation
                    Operation.I,  //expectedFeatureOperation
                    simpleFeature(),  //expectedFeature
                    1L,  //expectedVersion
                    Long.MAX_VALUE,  //expectedNextVersion
                    DEFAULT_AUTHOR,  //expectedAuthor
                    null  //expectedSQLError
                )
            },
            { "1", false, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.ERROR, null, null, null, null,  new Expectations(SQLError.FEATURE_NOT_EXISTS) },
            { "2", false, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.RETAIN, null, null, null, null, null },

       /** Feature exists + no ConflictHandling */
            { "3", false, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, null, null, null, null },
//          { "4", false, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, null, null },
            { "5", false, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, null, null, null,
                    new Expectations(
                            TableOperation.INSERT,  //expectedTableOperation
                            Operation.I,  //expectedFeatureOperation
                            simpleFeature(),  //expectedFeature
                            1L,  //expectedVersion
                            Long.MAX_VALUE,  //expectedNextVersion
                            DEFAULT_AUTHOR,  //expectedAuthor
                            null  //expectedSQLError
                    )
            },
            { "6", false, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, null, null, null,
                new Expectations(
                    TableOperation.INSERT,  //expectedTableOperation
                    Operation.I,  //expectedFeatureOperation
                    simpleFeature(),  //expectedFeature
                    1L,  //expectedVersion
                    Long.MAX_VALUE,  //expectedNextVersion
                    DEFAULT_AUTHOR,  //expectedAuthor
                    SQLError.FEATURE_EXISTS  //expectedSQLError
                )
            },
//
        /** Feature exists + With ConflictHandling + Baseversion MATCH */
//          { "7", false, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, OnVersionConflict.REPLACE, null, null, null, null, null, null, null /*TBD*/ },
//          { "8", false, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.REPLACE, null, null, null, null, null, null, null /*TBD*/ },
//          { "9", false, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, OnVersionConflict.REPLACE, null, null, null, null, null, null, null /*TBD*/ },
//          { "10", false, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, OnVersionConflict.REPLACE, null, null, null, null, null, null, null /*TBD*/ },
//
        /** Feature exists + With ConflictHandling + BaseVersion MISSMATCH*/
//           { "11", false, true, true, true, null, null, null, UserIntent.WRITE, null, null, OnVersionConflict.ERROR, null, null, null, null, null, null, null /*TBD*/ },
//           { "12", false, true, true, true, null, null, null, UserIntent.WRITE, null, null, OnVersionConflict.RETAIN, null, null, null, null, null, null, null /*TBD*/ },
//
        /** Feature exists + With ConflictHandling + Baseversion MISSMATCH -> REPLACE*/
//           { "13", false, true, true, false, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, OnVersionConflict.REPLACE, null, null, null, null, null, null, null /*TBD*/ },
//           { "14", false, true, true, false, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.REPLACE, null, null, null, null, null, null, null /*TBD*/ },
//           { "15", false, true, true, false, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, OnVersionConflict.REPLACE, null, null, null, null, null, null, null /*TBD*/ },
//           { "16", false, true, true, false, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, OnVersionConflict.REPLACE, null, null, null, null, null, null, null /*TBD*/ },
//
        /** Feature exists + With ConflictHandling + Baseversion MISSMATCH -> MERGE NonConflicting*/
//           { "17", false, true, true, false, false, null, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, null, null, null, null, null, null, null /*TBD*/ },
//
        /** Feature exists + With ConflictHandling + Baseversion MISSMATCH -> MERGE Conflicting*/
//           { "18", false, true, true, false, true, null, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, OnMergeConflict.ERROR, null, null, null, null, null, null /*TBD*/ },
//           { "19", false, true, true, false, true, null, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, OnMergeConflict.RETAIN, null, null, null, null, null, null /*TBD*/ },
//           { "20", false, true, true, false, true, null, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, OnMergeConflict.REPLACE, null, null, null, null, null, null /*TBD*/ },
        });
    }

    @Test
    public void start() throws Exception {
        testFeatureWriterTextExecutor();
    }
}
