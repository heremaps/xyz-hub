package com.here.xyz.test.featurewriter.noncomposite.nohistory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.test.GenericSpaceBased;
import com.here.xyz.test.GenericSpaceBased.OnExists;
import com.here.xyz.test.GenericSpaceBased.OnMergeConflict;
import com.here.xyz.test.GenericSpaceBased.OnNotExists;
import com.here.xyz.test.GenericSpaceBased.OnVersionConflict;
import com.here.xyz.test.GenericSpaceBased.Operation;
import com.here.xyz.test.GenericSpaceBased.SQLError;
import com.here.xyz.test.featurewriter.SQLBasedTestSuite;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class SQLNonCompositeNoHistoryTestSuiteIT extends SQLBasedTestSuite {

    public SQLNonCompositeNoHistoryTestSuiteIT(String testName, boolean composite, boolean history, boolean featureExists,
                                               Boolean baseVersionMatch, Boolean conflictingAttributes, Boolean featureExistsInSuper, Boolean featureExistsInExtension,
                                               UserIntent userIntent, OnNotExists onNotExists, OnExists onExists, OnVersionConflict onVersionConflict,
                                               OnMergeConflict onMergeConflict, SpaceContext spaceContext, Expectations expectations) {
        super(testName, composite, history, featureExists, baseVersionMatch, conflictingAttributes, featureExistsInSuper, featureExistsInExtension,
                userIntent, onNotExists, onExists, onVersionConflict, onMergeConflict, spaceContext, expectations);
    }

    @Parameterized.Parameters(name = "Test Number #{0}")
    public static Collection<Object[]> testScenarios() throws JsonProcessingException {
        return Arrays.asList(new Object[][]{
             /** Feature not exists */
             {
             "0",    //testName
             false,  //composite
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
                 GenericSpaceBased.DEFAULT_AUTHOR,  //expectedAuthor
                 null  //expectedSQLError
             )
         },
         /* No existing Feature expected. No TableOperation!  */
         { "2", false, false, false, null, null, null, null, UserIntent.WRITE, OnNotExists.ERROR, null, null, null, null, new Expectations(SQLError.FEATURE_NOT_EXISTS) },
         /* No existing Feature expected. No TableOperation!  */
         { "3", false, false, false, null, null, null, null, UserIntent.WRITE, OnNotExists.RETAIN, null, null, null, null, null },

         /** Feature exists */
         /* No existing Feature expected */
         { "4", false, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, null, null, null, new Expectations(TableOperation.DELETE) },
         { "5", false, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, null,
                 /* Expected content of the replaced Feature */
                 new Expectations(
                         TableOperation.UPDATE,  //expectedTableOperation
                         Operation.U,  //expectedFeatureOperation
                         simple1thModificatedFeature(),  //expectedFeature
                         2L,  //expectedVersion
                         Long.MAX_VALUE,  //expectedNextVersion
                         GenericSpaceBased.UPDATE_AUTHOR,  //expectedAuthor
                         null  //expectedSQLError
                 )
         },
         { "6", false, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, null, null, null,
                 /* Expected content of first written Feature which should stay untouched. No TableOperation! */
                 new Expectations(
                         TableOperation.INSERT,  //expectedTableOperation
                         Operation.I,  //expectedFeatureOperation
                         simple1thModificatedFeature(),  //expectedFeature
                         1L,  //expectedVersion
                         Long.MAX_VALUE,  //expectedNextVersion
                         GenericSpaceBased.DEFAULT_AUTHOR,  //expectedAuthor
                         null  //expectedSQLError
                 )
         },
         { "7", false, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, null, null, null,
                 /* SQLError.FEATURE_EXISTS & Expected content of first written Feature which should stay untouched. No TableOperation! */
                 new Expectations(
                         TableOperation.INSERT,  //expectedTableOperation
                         Operation.I,  //expectedFeatureOperation
                         simpleFeature(),  //expectedFeature
                         1L,  //expectedVersion
                         Long.MAX_VALUE,  //expectedNextVersion
                         GenericSpaceBased.DEFAULT_AUTHOR,  //expectedAuthor
                         SQLError.FEATURE_EXISTS  //expectedSQLError
                 )
         },

         /** Feature exists and got updated. Third write will have a Baseversion MATCH */
         /* No existing Feature expected */
         { "8", false, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, OnVersionConflict.REPLACE, null, null, new Expectations(TableOperation.DELETE) },
         { "9", false, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.REPLACE, null, null,
                 /* Expected content of the replaced Feature (3th write). */
                 new Expectations(
                         TableOperation.UPDATE,  //expectedTableOperation
                         Operation.U,  //expectedFeatureOperation
                         simple2thModificatedFeature(3L, false),  //expectedFeature
                         3L,  //expectedVersion
                         Long.MAX_VALUE,  //expectedNextVersion
                         GenericSpaceBased.UPDATE_AUTHOR,  //expectedAuthor
                         null  //expectedSQLError
                 )
         },
        { "10", false, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, OnVersionConflict.REPLACE, null, null,
                /* Expected content of the untouched Feature (from 2th write). No TableOperation! */
                 new Expectations(
                         TableOperation.INSERT,  //expectedTableOperation
                         Operation.U,  //expectedFeatureOperation
                         simple1thModificatedFeature(),  //expectedFeature
                         2L,  //expectedVersion
                         Long.MAX_VALUE,  //expectedNextVersion
                         GenericSpaceBased.DEFAULT_AUTHOR,  //expectedAuthor
                         null  //expectedSQLError
                 )
         },
         { "11", false, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, OnVersionConflict.REPLACE, null, null,
                 /* SQLError.FEATURE_EXISTS & Expected content of the untouched Feature (from 2th write). No TableOperation! */
                 new Expectations(
                         TableOperation.INSERT,  //expectedTableOperation
                         Operation.U,  //expectedFeatureOperation
                         simple1thModificatedFeature(),  //expectedFeature
                         2L,  //expectedVersion
                         Long.MAX_VALUE,  //expectedNextVersion
                         GenericSpaceBased.DEFAULT_AUTHOR,  //expectedAuthor
                         SQLError.FEATURE_EXISTS  //expectedSQLError
                 ),
         },
         /** Feature exists and got updated. Third write will have a Baseversion MISSMATCH */
         { "12", false, false, true, false, null, null, null, UserIntent.WRITE, null, null, OnVersionConflict.ERROR, null, null,
                 /* SQLError.VERSION_CONFLICT_ERROR & Expected content of the untouched Feature (from 2th write). No TableOperation! */
                 new Expectations(
                         TableOperation.INSERT,  //expectedTableOperation
                         Operation.U,  //expectedFeatureOperation
                         simple1thModificatedFeature(),  //expectedFeature
                         2L,  //expectedVersion
                         Long.MAX_VALUE,  //expectedNextVersion
                         GenericSpaceBased.DEFAULT_AUTHOR,  //expectedAuthor
                         SQLError.VERSION_CONFLICT_ERROR  //expectedSQLError
                 ),
         },
         { "13", false, false, true, false, null, null, null, UserIntent.WRITE, null, null, OnVersionConflict.RETAIN, null, null,
                 /* Expected content of the untouched Feature (from 2th write). No TableOperation! */
                 new Expectations(
                         TableOperation.INSERT,  //expectedTableOperation
                         Operation.U,  //expectedFeatureOperation
                         simple1thModificatedFeature(),  //expectedFeature
                         2L,  //expectedVersion
                         Long.MAX_VALUE,  //expectedNextVersion
                         GenericSpaceBased.DEFAULT_AUTHOR,  //expectedAuthor
                         null  //expectedSQLError
                 ),
         },
         { "14", false, false, true, false, null, null, null, UserIntent.WRITE, null, null, OnVersionConflict.REPLACE, null, null,
                 /* Expected content of the replaced Feature (from 3th write). */
                 new Expectations(
                         TableOperation.UPDATE,  //expectedTableOperation
                         Operation.U,  //expectedFeatureOperation
                         simple2thModificatedFeature(3L, false),  //expectedFeature
                         3L,  //expectedVersion
                         Long.MAX_VALUE,  //expectedNextVersion
                         GenericSpaceBased.UPDATE_AUTHOR,  //expectedAuthor
                         null  //expectedSQLError
                 ),
         },
        { "15", false, false, true, false, null, null, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, null, null,
                /* SQLError.ILLEGAL_ARGUMEN & Expected content of the untouched Feature (from 2th write). No TableOperation!  */
                new Expectations(
                    TableOperation.INSERT,  //expectedTableOperation
                    Operation.U,  //expectedFeatureOperation
                    simple1thModificatedFeature(),  //expectedFeature
                    2L,  //expectedVersion
                    Long.MAX_VALUE,  //expectedNextVersion
                    GenericSpaceBased.DEFAULT_AUTHOR,  //expectedAuthor
                    SQLError.ILLEGAL_ARGUMENT  //expectedSQLError
                ),
            },
        });
    }

    @Test
    public void start() throws Exception {
        featureWriterExecutor();
    }
}
