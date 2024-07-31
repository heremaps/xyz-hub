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
public class CompositNoHistoryTestSuiteIT extends TestSuiteIT {

    public CompositNoHistoryTestSuiteIT(boolean composite, boolean history, boolean featureExists, Boolean baseVersionMatch, Boolean conflictingAttributes, Boolean featureExistsInSuper, Boolean featureExistsInExtension, UserIntent userIntent, OnNotExists onNotExists, OnExists onExists, OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, SpaceContext spaceContext, Expectations expectations) {
        super(composite, history, featureExists, baseVersionMatch, conflictingAttributes, featureExistsInSuper, featureExistsInExtension, userIntent, onNotExists, onExists, onVersionConflict, onMergeConflict, spaceContext, expectations);
    }

    @Parameterized.Parameters(name = "{index} => ({1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},..);")
    public static Collection<Object[]> testScenarios() throws JsonProcessingException {
        return Arrays.asList(new Object[][]{

        });
    }

    @Test
    public void start() throws Exception {
        testFeatureWriterTextExecutor();
    }
}
