package com.here.xyz.test.featurewriter;

import com.amazonaws.services.dynamodbv2.xspec.B;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import org.junit.After;
import org.junit.Before;

public class TestSuiteIT extends SQLITWriteFeaturesBase{
    protected final static String DEFAULT_AUTHOR = "author";

    private String testName;
    private boolean composite;
    private boolean history;
    private boolean featureExists;
    private Boolean baseVersionMatch;
    private Boolean conflictingAttributes;
    private Boolean featureExistsInSuper;
    private Boolean featureExistsInExtension;

    private UserIntent userIntent;
    private OnNotExists onNotExists;
    private OnExists onExists;
    private OnVersionConflict onVersionConflict;
    private OnMergeConflict onMergeConflict;
    private SpaceContext spaceContext;

    private Expectations expectations;

    public TestSuiteIT(String testName, boolean composite, boolean history, boolean featureExists, Boolean baseVersionMatch, Boolean conflictingAttributes, Boolean featureExistsInSuper,
                       Boolean featureExistsInExtension, UserIntent userIntent, OnNotExists onNotExists, OnExists onExists, OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict,
                       SpaceContext spaceContext, Expectations expectations) {
        this.testName = testName;
        this.composite = composite;
        this.history = history;
        this.featureExists = featureExists;
        this.baseVersionMatch = baseVersionMatch;
        this.conflictingAttributes = conflictingAttributes;
        this.featureExistsInSuper = featureExistsInSuper;
        this.featureExistsInExtension = featureExistsInExtension;

        this.userIntent = userIntent;
        this.onNotExists = onNotExists;
        this.onExists = onExists;
        this.onVersionConflict = onVersionConflict;
        this.onMergeConflict = onMergeConflict;
        this.spaceContext = spaceContext;

        this.expectations = expectations;
    }

    protected enum TableOperation{
        INSERT,
        UPDATE,
        DELETE
    }

    protected enum UserIntent {
        WRITE, //Illegal Argument
        DELETE
    }

    @Before
    public void prepare() throws Exception {
        createSpaceTable(this.composite);
    }

    @After
    public void clean() throws Exception {
        dropSpaceTable(this.composite);
    }

    protected static Feature simpleFeature() throws JsonProcessingException {
        return XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{"firstName":"Alice","age":35}
            }
            """, Feature.class);
    }

    protected static Feature simple1thModificatedFeature() throws JsonProcessingException {
        return simple1thModificatedFeature(null);
    }

    private static Feature simple1thModificatedFeature(Long version) throws JsonProcessingException {
       Feature feature = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{"firstName":"Alice","age":35, "lastName":"Wonder"}
            }
            """, Feature.class);

       if(version != null){
           feature.getProperties().withXyzNamespace(new XyzNamespace().withVersion(version));
       }
       return feature;
    }

    protected static Feature simple2thModificatedFeature(Long version, Boolean conflictingAttributes) throws JsonProcessingException {
        Feature feature = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{"age":"32"}
            }
            """, Feature.class);

        if(conflictingAttributes != null && conflictingAttributes){
            feature.getProperties().with("lastName","NotWonder");
        }

        if(version != null){
            feature.getProperties().withXyzNamespace(new XyzNamespace().withVersion(version));
        }
        return feature;
    }

    public record Expectations(TableOperation tableOperation, Operation featureOperation,
                                Feature feature, long version, long nextVersion, String author,
                                SQLError sqlError) {
        public Expectations(SQLError sqlError){
            this(null, null, null, 0L, 0L, null, sqlError);
        }
    }

    public void featureWriterExecutor() throws Exception {
        if(baseVersionMatch != null){
            featureWriterExecutor_WithBaseVersion();
        }else
            featureWriterExecutor_WithoutBaseVersion();
    }

    public void featureWriterExecutor_WithoutBaseVersion() throws Exception {
        //TODO: Check UserIntent
        if(this.spaceContext == null)
            this.spaceContext = SpaceContext.EXTENSION;

        if(!this.featureExists) {
            writeFeature(simpleFeature(), DEFAULT_AUTHOR, onExists, onNotExists,
                    onVersionConflict, onMergeConflict, false, spaceContext, history, expectations != null ? expectations.sqlError() : null);
        }else{
            //Simple 1th write
            writeFeature(simpleFeature(), DEFAULT_AUTHOR, null, null,
                    null, null, false, SpaceContext.EXTENSION, history, null);
            //2th write
            writeFeature(simple1thModificatedFeature(), UPDATE_AUTHOR, onExists, onNotExists,
                    onVersionConflict, onMergeConflict, false, spaceContext, history, expectations != null ? expectations.sqlError() : null);
        }

        checkStrategies();
    }

    public void featureWriterExecutor_WithBaseVersion() throws Exception {
        //TODO: Check UserIntent
        if(this.spaceContext == null)
            this.spaceContext = SpaceContext.EXTENSION;

        if(!this.featureExists) {
            writeFeature(simpleFeature(), DEFAULT_AUTHOR, onExists, onNotExists,
                    onVersionConflict, onMergeConflict, false, spaceContext, history, expectations != null ? expectations.sqlError() : null);
        }else{
            //Simple 1th write
            writeFeature(simpleFeature(), DEFAULT_AUTHOR, null, null,
                    null, null, false, SpaceContext.EXTENSION, history, null);
            //Simple 2th write
            writeFeature(simple1thModificatedFeature(1L), DEFAULT_AUTHOR, null, null,
                    null, null, false, SpaceContext.EXTENSION, history, null);
            //3th write
            Long version = baseVersionMatch != null ? (baseVersionMatch ? 2L : 1L) : null;
            if( baseVersionMatch != null){
                writeFeature(simple2thModificatedFeature(version, conflictingAttributes), UPDATE_AUTHOR, onExists, onNotExists,
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
                case CREATE -> checkExistingFeature(expectations.feature(), expectations.version(), expectations.nextVersion(),
                        expectations.featureOperation(),expectations.author());
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

    private void checkOnExistsDelete() throws Exception {
        if(history){
            checkDeletedFeatureOnHistory(simpleFeature().getId(), true);

            if(baseVersionMatch != null)
                //1th insert, 2th update, 3th delete
                checkFeatureCount(3);
            else
                //1th insert, 2th delete
                checkFeatureCount(2);
        }else{
            checkNotExistingFeature(simpleFeature().getId());
            //Feature should not exist in the table
            checkFeatureCount(0);
        }
    }

    private void checkRetainOrError() throws Exception {
        if(this.featureExists) {
            checkExistingFeature(expectations.feature(), expectations.version(), expectations.nextVersion(),
                    expectations.featureOperation(), expectations.author());
        }else{
            checkNotExistingFeature(simpleFeature().getId());
        }
        checkFeatureCountsForRetainOrError();
    }

    private void checkFeaturesOnReplace() throws Exception {
        if(history) {
            //First Write
            checkExistingFeature(simpleFeature(), 1L, 2L, Operation.I, DEFAULT_AUTHOR);
            //Last Write
            checkExistingFeature(expectations.feature(), expectations.version(), expectations.nextVersion(),
                    expectations.featureOperation(), expectations.author());
        }else {
            //Head
            checkExistingFeature(expectations.feature(), expectations.version(), expectations.nextVersion(),
                    expectations.featureOperation(), expectations.author());
        }
        checkFeatureCountsForReplace();
    }


    private void checkFeaturesOnMerge() throws Exception {
        //In our merge tests we have at least one feature
        if(history) {
            //First Write
            checkExistingFeature(simpleFeature(), 1L, 2L, Operation.I, DEFAULT_AUTHOR);
            //Last Write
            checkExistingFeature(expectations.feature(), expectations.version(), expectations.nextVersion(),
                    expectations.featureOperation(), expectations.author());
            if(onMergeConflict != null && (onMergeConflict.equals(OnMergeConflict.ERROR) || onMergeConflict.equals(OnMergeConflict.RETAIN))) {
                //1th insert / 2th update
                checkFeatureCount(2);
            }else
                //1th insert / 2th update / 3th merge
                checkFeatureCount(3);
        }else {
            //Head
            checkExistingFeature(expectations.feature(), expectations.version(), expectations.nextVersion(),
                    expectations.featureOperation(), expectations.author());
            checkFeatureCount(1);
        }
    }

/** Count checkers */
    private void checkFeatureCountsForReplace() throws Exception {
        if(history){
            if(baseVersionMatch != null)
                //1th insert, 2th update, 3th Update
                checkFeatureCount(3);
            else
                //1th insert, 2th update
                checkFeatureCount(2);
        }else{
            //Head
            checkFeatureCount(1);
        }
    }

    private void checkFeatureCountsForRetainOrError() throws Exception {
        if(this.featureExists) {
            if(history){
                if(baseVersionMatch != null)
                    //1th insert, 2th update
                    checkFeatureCount(2);
                else
                    //1th insert
                    checkFeatureCount(1);
            }else{
                //Feature should exist in the table
                checkFeatureCount(1);
            }
        }else{
            if(history){
                if(baseVersionMatch != null)
                    //1th insert
                    checkFeatureCount(1);
                else
                    //Feature should not exist in the table
                    checkFeatureCount(0);
            }else{
                //Feature should not exist in the table
                checkFeatureCount(0);
            }
        }
    }

}
