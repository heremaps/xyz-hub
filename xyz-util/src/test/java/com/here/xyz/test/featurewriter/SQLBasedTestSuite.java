package com.here.xyz.test.featurewriter;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.test.GenericSpaceBased;
import com.here.xyz.test.SQLBasedSpaceTest;
import org.junit.After;
import org.junit.Before;

public class SQLBasedTestSuite extends TestSuite{
    protected GenericSpaceBased genericSpaceWriter;

    @Before
    public void prepare() throws Exception {
        genericSpaceWriter.createSpaceResources();
    }

    @After
    public void clean() throws Exception {
        genericSpaceWriter.cleanSpaceResources();
    }

    public SQLBasedTestSuite(String testName, boolean composite, boolean history, boolean featureExists, Boolean baseVersionMatch, Boolean conflictingAttributes, Boolean featureExistsInSuper, Boolean featureExistsInExtension, UserIntent userIntent, GenericSpaceBased.OnNotExists onNotExists, GenericSpaceBased.OnExists onExists, GenericSpaceBased.OnVersionConflict onVersionConflict, GenericSpaceBased.OnMergeConflict onMergeConflict, SpaceContext spaceContext, Expectations expectations) {
        super(testName, composite, history, featureExists, baseVersionMatch, conflictingAttributes, featureExistsInSuper, featureExistsInExtension, userIntent, onNotExists, onExists, onVersionConflict, onMergeConflict, spaceContext, expectations);
        genericSpaceWriter = new SQLBasedSpaceTest(composite);
    }

    public void featureWriterExecutor() throws Exception {
        if(baseVersionMatch != null){
            featureWriterExecutor_WithBaseVersion();
        }else
            featureWriterExecutor_WithoutBaseVersion();
    }

    public void featureWriterExecutor_WithoutBaseVersion() throws Exception {
        //TODO: Check UserIntent

        if(!this.featureExists) {
            genericSpaceWriter.writeFeature(simpleFeature(), GenericSpaceBased.DEFAULT_AUTHOR, onExists, onNotExists,
                    onVersionConflict, onMergeConflict, false, spaceContext, history, expectations != null ? expectations.sqlError() : null);
        }else{
            //Simple 1th write
            genericSpaceWriter.writeFeature(simpleFeature(), GenericSpaceBased.DEFAULT_AUTHOR, null, null,
                    null, null, false, SpaceContext.EXTENSION, history, null);
            //2th write
            genericSpaceWriter.writeFeature(simple1thModificatedFeature(), GenericSpaceBased.UPDATE_AUTHOR, onExists, onNotExists,
                    onVersionConflict, onMergeConflict, false, spaceContext, history, expectations != null ? expectations.sqlError() : null);
        }
        checkStrategies();
    }

    public void featureWriterExecutor_WithBaseVersion() throws Exception {
        //TODO: Check UserIntent

        if(!this.featureExists) {
            genericSpaceWriter.writeFeature(simpleFeature(), GenericSpaceBased.DEFAULT_AUTHOR, onExists, onNotExists,
                    onVersionConflict, onMergeConflict, false, spaceContext, history, expectations != null ? expectations.sqlError() : null);
        }else{
            //Simple 1th write
            genericSpaceWriter.writeFeature(simpleFeature(), GenericSpaceBased.DEFAULT_AUTHOR, null, null,
                    null, null, false, SpaceContext.EXTENSION, history, null);
            //Simple 2th write
            genericSpaceWriter.writeFeature(simple1thModificatedFeature(1L), GenericSpaceBased.DEFAULT_AUTHOR, null, null,
                    null, null, false, SpaceContext.EXTENSION, history, null);
            //3th write
            Long version = baseVersionMatch != null ? (baseVersionMatch ? 2L : 1L) : null;
            if( baseVersionMatch != null){
                genericSpaceWriter.writeFeature(simple2thModificatedFeature(version, conflictingAttributes), GenericSpaceBased.UPDATE_AUTHOR, onExists, onNotExists,
                        onVersionConflict, onMergeConflict, false, spaceContext, history, expectations != null ? expectations.sqlError() : null);
            }
        }
        checkStrategies();
    }

    private void checkStrategies() throws Exception {
        if(onExists != null){
            switch(onExists){
                case ERROR, RETAIN -> checkRetainOrError();
                case DELETE -> checkOnExistsDelete();
                case REPLACE -> checkFeaturesOnReplace();
            }
        }

        if(onNotExists != null){
            switch(onNotExists){
                case ERROR, RETAIN -> checkRetainOrError();
                case CREATE -> checkOnNotExistsCreate();
            }
        }

        if(onVersionConflict != null){
            switch(onVersionConflict){
                case ERROR, RETAIN -> checkRetainOrError();
                case MERGE -> checkFeaturesOnMerge();
                case REPLACE -> {
                    //Has priority
                    if(onExists != null)
                        break;
                    checkFeaturesOnReplace();
                }
            }
        }

        if(onMergeConflict != null){
            switch(onMergeConflict){
                case ERROR, RETAIN -> checkRetainOrError();
                case REPLACE -> checkFeaturesOnReplace();
            }
        }
    }

    private void checkOnNotExistsCreate() throws Exception {
        genericSpaceWriter.checkExistingFeature(expectations.feature(), expectations.version(), expectations.nextVersion(),
                expectations.featureOperation(),expectations.author());
        genericSpaceWriter.checkFeatureCount(1);
    }

    private void checkOnExistsDelete() throws Exception {
        if(history){
            genericSpaceWriter.checkDeletedFeatureOnHistory(simpleFeature().getId(), true);

            if(baseVersionMatch != null)
                //1th insert, 2th update, 3th delete
                genericSpaceWriter.checkFeatureCount(3);
            else
                //1th insert, 2th delete
                genericSpaceWriter. checkFeatureCount(2);
        }else{
            genericSpaceWriter.checkNotExistingFeature(simpleFeature().getId());
            //Feature should not exist in the table
            genericSpaceWriter.checkFeatureCount(0);
        }
    }

    private void checkRetainOrError() throws Exception {
        if(this.featureExists) {
            genericSpaceWriter.checkExistingFeature(expectations.feature(), expectations.version(), expectations.nextVersion(),
                    expectations.featureOperation(), expectations.author());
        }else{
            genericSpaceWriter.checkNotExistingFeature(simpleFeature().getId());
        }
        checkFeatureCountsForRetainOrError();
    }

    private void checkFeaturesOnReplace() throws Exception {
        if(history) {
            //First Write
            genericSpaceWriter.checkExistingFeature(simpleFeature(), 1L, 2L, GenericSpaceBased.Operation.I, GenericSpaceBased.DEFAULT_AUTHOR);
            //Last Write
            genericSpaceWriter.checkExistingFeature(expectations.feature(), expectations.version(), expectations.nextVersion(),
                    expectations.featureOperation(), expectations.author());
        }else {
            //Head
            genericSpaceWriter.checkExistingFeature(expectations.feature(), expectations.version(), expectations.nextVersion(),
                    expectations.featureOperation(), expectations.author());
        }
        checkFeatureCountsForReplace();
    }


    private void checkFeaturesOnMerge() throws Exception {
        //In our merge tests we have at least one feature
        if(history) {
            //First Write
            genericSpaceWriter.checkExistingFeature(simpleFeature(), 1L, 2L, GenericSpaceBased.Operation.I, GenericSpaceBased.DEFAULT_AUTHOR);
            //Last Write
            genericSpaceWriter.checkExistingFeature(expectations.feature(), expectations.version(), expectations.nextVersion(),
                    expectations.featureOperation(), expectations.author());
            if(onMergeConflict != null && (onMergeConflict.equals(GenericSpaceBased.OnMergeConflict.ERROR) || onMergeConflict.equals(GenericSpaceBased.OnMergeConflict.RETAIN))) {
                //1th insert / 2th update
                genericSpaceWriter.checkFeatureCount(2);
            }else
                //1th insert / 2th update / 3th merge
                genericSpaceWriter. checkFeatureCount(3);
        }else {
            //Head
            genericSpaceWriter.checkExistingFeature(expectations.feature(), expectations.version(), expectations.nextVersion(),
                    expectations.featureOperation(), expectations.author());
            genericSpaceWriter.checkFeatureCount(1);
        }
    }

    /** Count checkers */
    private void checkFeatureCountsForReplace() throws Exception {
        if(history){
            if(baseVersionMatch != null)
                //1th insert, 2th update, 3th Update
                genericSpaceWriter.checkFeatureCount(3);
            else
                //1th insert, 2th update
                genericSpaceWriter.checkFeatureCount(2);
        }else{
            //Head
            genericSpaceWriter.checkFeatureCount(1);
        }
    }

    private void checkFeatureCountsForRetainOrError() throws Exception {
        if(this.featureExists) {
            if(history){
                if(baseVersionMatch != null)
                    //1th insert, 2th update
                    genericSpaceWriter.checkFeatureCount(2);
                else
                    //1th insert
                    genericSpaceWriter.checkFeatureCount(1);
            }else{
                //Feature should exist in the table
                genericSpaceWriter.checkFeatureCount(1);
            }
        }else{
            if(history){
                if(baseVersionMatch != null)
                    //1th insert
                    genericSpaceWriter.checkFeatureCount(1);
                else
                    //Feature should not exist in the table
                    genericSpaceWriter.checkFeatureCount(0);
            }else{
                //Feature should not exist in the table
                genericSpaceWriter.checkFeatureCount(0);
            }
        }
    }
}
