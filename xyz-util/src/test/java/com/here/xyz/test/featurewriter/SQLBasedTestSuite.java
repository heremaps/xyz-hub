package com.here.xyz.test.featurewriter;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.test.GenericSpaceBased;
import com.here.xyz.test.SQLBasedSpaceTest;

public class SQLBasedTestSuite extends TestSuite{
     public SQLBasedTestSuite(String testName, boolean composite, boolean history, boolean featureExists, Boolean baseVersionMatch, Boolean conflictingAttributes, Boolean featureExistsInSuper, Boolean featureExistsInExtension, UserIntent userIntent, GenericSpaceBased.OnNotExists onNotExists, GenericSpaceBased.OnExists onExists, GenericSpaceBased.OnVersionConflict onVersionConflict, GenericSpaceBased.OnMergeConflict onMergeConflict, SpaceContext spaceContext, Expectations expectations) {
        super(testName, composite, history, featureExists, baseVersionMatch, conflictingAttributes, featureExistsInSuper, featureExistsInExtension, userIntent, onNotExists, onExists, onVersionConflict, onMergeConflict, spaceContext, expectations);
        genericSpaceWriter = new SQLBasedSpaceTest(composite);
    }
}
