package com.here.xyz.test.featurewriter.composite.nohistory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.test.featurewriter.TestSuiteIT;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class CompositeNoHistoryTestSuiteIT extends TestSuiteIT {

    public CompositeNoHistoryTestSuiteIT(String testName, boolean composite, boolean history, boolean featureExists,
                                             Boolean baseVersionMatch, Boolean conflictingAttributes, Boolean featureExistsInSuper, Boolean featureExistsInExtension,
                                             UserIntent userIntent, OnNotExists onNotExists, OnExists onExists, OnVersionConflict onVersionConflict,
                                             OnMergeConflict onMergeConflict, SpaceContext spaceContext, TestSuiteIT.Expectations expectations) {
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
        testFeatureWriterTextExecutor();
    }
}
