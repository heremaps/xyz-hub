package com.here.xyz.test.featurewriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import org.junit.After;
import org.junit.Before;

public class TestSuiteIT extends SQLITWriteFeaturesBase{
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

    public TestSuiteIT(boolean composite, boolean history, boolean featureExists, Boolean baseVersionMatch, Boolean conflictingAttributes, Boolean featureExistsInSuper,
                       Boolean featureExistsInExtension, UserIntent userIntent, OnNotExists onNotExists, OnExists onExists, OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict,
                       SpaceContext spaceContext, Expectations expectations) {
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

    protected static Feature simpleModifiedFeature() throws JsonProcessingException {
        return simpleModifiedFeature(null);
    }

    private static Feature simpleModifiedFeature(Long version) throws JsonProcessingException {
       Feature feature = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{"lastName":"Wonder"}
            }
            """, Feature.class);

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

    public void testFeatureWriterTextExecutor() throws Exception {
        //TODO: Check UserIntent
        if(this.spaceContext == null)
            this.spaceContext = SpaceContext.EXTENSION;

        if(!this.featureExists) {
            writeFeature(simpleFeature(), DEFAULT_AUTHOR, onExists, onNotExists,
                    onVersionConflict, onMergeConflict, false, spaceContext, history, expectations != null ? expectations.sqlError() : null);
        }else{
            //Simple 1th write
            writeFeature(simpleFeature(), DEFAULT_AUTHOR, null, null,
                    null, null, false, SpaceContext.EXTENSION, false, null);
            //2th write
            Long version = baseVersionMatch != null ? (baseVersionMatch ? 1L : 0L) : null;

            writeFeature(simpleModifiedFeature(version), UPDATE_AUTHOR, onExists, onNotExists,
                    onVersionConflict, onMergeConflict, false, spaceContext, history, expectations != null ? expectations.sqlError() : null);
        }

        if(onExists != null && onExists.equals(OnExists.RETAIN)
                || onVersionConflict != null && onVersionConflict.equals(OnVersionConflict.RETAIN)
                || onMergeConflict != null && onMergeConflict.equals(onMergeConflict.RETAIN)){
            //On RETAIN Strategy (feature exists) we expect no write and the existence of the old feature
            checkExistingFeature(expectations.feature(), expectations.version(), expectations.nextVersion(),
                    expectations.featureOperation(),expectations.author());
        }else if(onNotExists != null && onNotExists.equals(OnNotExists.RETAIN)
            || onExists != null && onExists.equals(OnExists.DELETE)){
            //On RETAIN Strategy (feature not exists) we expect no write
            checkNotExistingFeature(simpleFeature().getId());
        }
    }
}
