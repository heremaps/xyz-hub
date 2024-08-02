package com.here.xyz.test.featurewriter.composite.nohistory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.test.GenericSpaceBased;
import com.here.xyz.test.GenericSpaceBased.OnExists;
import com.here.xyz.test.GenericSpaceBased.OnMergeConflict;
import com.here.xyz.test.GenericSpaceBased.OnNotExists;
import com.here.xyz.test.GenericSpaceBased.OnVersionConflict;
import com.here.xyz.test.GenericSpaceBased.Operation;
import com.here.xyz.test.featurewriter.noncomposite.nohistory.SQLNonCompositeNoHistoryTestSuiteIT;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class HubComposite_DEFAULT_NoHistoryTestSuiteIT extends SQLNonCompositeNoHistoryTestSuiteIT {

    public HubComposite_DEFAULT_NoHistoryTestSuiteIT(String testName, boolean composite, boolean history, boolean featureExists,
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
                        SpaceContext.DEFAULT,  //spaceContext

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
//                /* No existing Feature expected. No TableOperation!  */
//                { "2", true, false, false, null, null, null, SpaceContext.EXTENSION, UserIntent.WRITE, OnNotExists.ERROR, null, null, null, null, new Expectations(SQLError.FEATURE_NOT_EXISTS) },
//                /* No existing Feature expected. No TableOperation!  */
//                { "3", true, false, false, null, null, null, null, UserIntent.WRITE, OnNotExists.RETAIN, null, null, null, null, null },
        });
    }

    @Test
    public void start() throws Exception {
        featureWriterExecutor();
    }
}
