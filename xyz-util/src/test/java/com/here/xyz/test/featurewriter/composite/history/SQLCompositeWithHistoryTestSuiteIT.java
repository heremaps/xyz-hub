package com.here.xyz.test.featurewriter.composite.history;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.test.GenericSpaceBased.OnExists;
import com.here.xyz.test.GenericSpaceBased.OnMergeConflict;
import com.here.xyz.test.GenericSpaceBased.OnNotExists;
import com.here.xyz.test.GenericSpaceBased.OnVersionConflict;
import com.here.xyz.test.featurewriter.SQLBasedTestSuite;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class SQLCompositeWithHistoryTestSuiteIT extends SQLBasedTestSuite {

    public SQLCompositeWithHistoryTestSuiteIT(String testName, boolean composite, boolean history, boolean featureExists,
                                              Boolean baseVersionMatch, Boolean conflictingAttributes, Boolean featureExistsInSuper, Boolean featureExistsInExtension,
                                              UserIntent userIntent, OnNotExists onNotExists, OnExists onExists, OnVersionConflict onVersionConflict,
                                              OnMergeConflict onMergeConflict, SpaceContext spaceContext, Expectations expectations) {
        super(testName, composite, history, featureExists, baseVersionMatch, conflictingAttributes, featureExistsInSuper, featureExistsInExtension,
                userIntent, onNotExists, onExists, onVersionConflict, onMergeConflict, spaceContext, expectations);
    }

    @Parameterized.Parameters(name = "Test Number #{0}")
    public static Collection<Object[]> testScenarios() throws JsonProcessingException {
        return Arrays.asList(new Object[][]{

        });
    }

    @Test
    public void start() throws Exception {
        featureWriterExecutor();
    }
}
