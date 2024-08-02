package com.here.xyz.test.featurewriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.test.GenericSpaceBased.OnNotExists;
import com.here.xyz.test.GenericSpaceBased.OnExists;
import com.here.xyz.test.GenericSpaceBased.OnVersionConflict;
import com.here.xyz.test.GenericSpaceBased.OnMergeConflict;
import com.here.xyz.test.GenericSpaceBased.Operation;
import com.here.xyz.test.GenericSpaceBased.SQLError;

public class TestSuite {

    protected String testName;
    protected boolean composite;
    protected boolean history;
    protected boolean featureExists;
    protected Boolean baseVersionMatch;
    protected Boolean conflictingAttributes;
    protected Boolean featureExistsInSuper;
    protected Boolean featureExistsInExtension;

    protected UserIntent userIntent;
    protected OnNotExists onNotExists;
    protected OnExists onExists;
    protected OnVersionConflict onVersionConflict;
    protected OnMergeConflict onMergeConflict;
    protected SpaceContext spaceContext;

    protected Expectations expectations;

    public enum TableOperation{
        INSERT,
        UPDATE,
        DELETE
    }

    protected enum UserIntent {
        WRITE, //Illegal Argument
        DELETE
    }

    public record Expectations(TableOperation tableOperation, Operation featureOperation,
                               Feature feature, long version, long nextVersion, String author,
                               SQLError sqlError) {
        public Expectations(SQLError sqlError){
            this(null, null, null, 0L, 0L, null, sqlError);
        }
        public Expectations(TableOperation tableOperation){
            this(tableOperation, null, null, 0L, 0L, null, null);
        }
    }

    public TestSuite(String testName, boolean composite, boolean history, boolean featureExists, Boolean baseVersionMatch, Boolean conflictingAttributes, Boolean featureExistsInSuper,
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

        if(this.spaceContext == null)
            this.spaceContext = SpaceContext.EXTENSION;
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

    protected static Feature simple1thModificatedFeature(Long version) throws JsonProcessingException {
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
}
