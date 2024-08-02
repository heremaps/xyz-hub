package com.here.xyz.test.featurewriter.composite.history;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.test.GenericSpaceBased.OnExists;
import com.here.xyz.test.GenericSpaceBased.OnMergeConflict;
import com.here.xyz.test.GenericSpaceBased.OnNotExists;
import com.here.xyz.test.GenericSpaceBased.OnVersionConflict;
import com.here.xyz.test.featurewriter.noncomposite.history.SQLNonCompositWithHistoryTestSuiteIT;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SQLComposite_SUPER_WithHistoryTestSuiteIT extends SQLNonCompositWithHistoryTestSuiteIT {

    public SQLComposite_SUPER_WithHistoryTestSuiteIT(String testName, boolean composite, boolean history, boolean featureExists,
                                                     Boolean baseVersionMatch, Boolean conflictingAttributes, Boolean featureExistsInSuper, Boolean featureExistsInExtension,
                                                     UserIntent userIntent, OnNotExists onNotExists, OnExists onExists, OnVersionConflict onVersionConflict,
                                                     OnMergeConflict onMergeConflict, SpaceContext spaceContext, Expectations expectations) {
        super(testName, true, history, featureExists, baseVersionMatch, conflictingAttributes, featureExistsInSuper, featureExistsInExtension,
                userIntent, onNotExists, onExists, onVersionConflict, onMergeConflict, SpaceContext.SUPER, expectations);
    }

    //TODO: Alin Hub and featureWriter
    //It's not permitted to perform modifications through context SUPER.
//    @Test
//    public void start() throws Exception {
//        featureWriterExecutor();
//    }
}
