package com.here.xyz.test.featurewriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
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

    private TableOperation expectedTableOperation;
    private Operation expectedFeatureOperation;
    private SQLError expectedSQLError;
    private Feature expectedFeature;
    private long expectedVersion;
    private long expectedNextVersion;
    private String expectedAuthor;

    public TestSuiteIT(boolean composite, boolean history, boolean featureExists, Boolean baseVersionMatch, Boolean conflictingAttributes, Boolean featureExistsInSuper,
                       Boolean featureExistsInExtension, UserIntent userIntent, OnNotExists onNotExists, OnExists onExists, OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict,
                       SpaceContext spaceContext, TableOperation expectedTableOperation, Operation expectedFeatureOperation,
                       Feature expectedFeature, long expectedVersion, long expectedNextVersion, String expectedAuthor, SQLError expectedSQLError)
            throws JsonProcessingException {

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

        this.expectedTableOperation = expectedTableOperation;
        this.expectedFeatureOperation = expectedFeatureOperation;
        this.expectedFeature = expectedFeature;
        this.expectedVersion = expectedVersion;
        this.expectedNextVersion = expectedNextVersion;
        this.expectedAuthor = expectedAuthor;
        this.expectedSQLError = expectedSQLError;
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

    private static Feature simpleFeature() throws JsonProcessingException {
        return XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{"firstName":"Alice","age":35}
            }
            """, Feature.class);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testScenarios() throws JsonProcessingException {
        return Arrays.asList(new Object[][]{
                {   false,  //composite
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

                    TableOperation.INSERT,  //expectedTableOperation
                    Operation.I,  //expectedFeatureOperation
                    simpleFeature(),  //expectedFeature
                    1L,  //expectedVersion
                    Long.MAX_VALUE,  //expectedNextVersion
                    DEFAULT_AUTHOR,  //expectedAuthor
                    null  //expectedSQLError
                },
                {false, false, false, null, null, null, null, UserIntent.WRITE, OnNotExists.ERROR, null, null, null, SpaceContext.DEFAULT, null, null, SQLError.FEATURE_NOT_EXISTS},
//                {false, false, false, null, null, null, null, UserIntent.WRITE, OnNotExists.RETAIN, null, null, null, SpaceContext.DEFAULT, null, null, null},
//                {false, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, null, null, SpaceContext.DEFAULT, TableOperation.DELETE, null, null},
//                {false, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, SpaceContext.DEFAULT, TableOperation.UPDATE, Operation.U, null},
//                {false, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, null, null, SpaceContext.DEFAULT, null, null, null},
//                {false, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, null, null, SpaceContext.DEFAULT, null, null, SQLError.FEATURE_EXISTS},
//                {false, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, null, null, SpaceContext.DEFAULT, TableOperation.DELETE, null, null},
//                {false, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, SpaceContext.DEFAULT, TableOperation.UPDATE, Operation.U, null},
//                {false, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, null, null, SpaceContext.DEFAULT, null, null, null},
//                {false, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, null, null, SpaceContext.DEFAULT, null, null, SQLError.FEATURE_EXISTS},
//                {false, false, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.ERROR, null, SpaceContext.DEFAULT, null, null, SQLError.VERSION_CONFLICT_ERROR},
//                {false, false, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.RETAIN, null, SpaceContext.DEFAULT, null, null, null},
//                {false, false, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.REPLACE, null, SpaceContext.DEFAULT, TableOperation.UPDATE, Operation.U, null},
//                {false, false, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, null, SpaceContext.DEFAULT, null, null, SQLError.ILLEGAL_ARGUMENT},
//                {false, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.CREATE, null, null, null, SpaceContext.DEFAULT, TableOperation.INSERT, Operation.I, null},
//                {false, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.ERROR, null, null, null, SpaceContext.DEFAULT, null, null, SQLError.FEATURE_NOT_EXISTS},
//                {false, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.RETAIN, null, null, null, SpaceContext.DEFAULT, null, null, null},
//                {false, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, null, null, SpaceContext.DEFAULT, TableOperation.DELETE, null, null},
//                {false, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, SpaceContext.DEFAULT, TableOperation.UPDATE, Operation.U, null},
//                {false, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, null, null, SpaceContext.DEFAULT, null, null, null},
//                {false, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, null, null, SpaceContext.DEFAULT, null, null, SQLError.FEATURE_EXISTS},
//                {false, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, null, null, SpaceContext.DEFAULT, TableOperation.DELETE, null, null},
//                {false, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, SpaceContext.DEFAULT, TableOperation.UPDATE, Operation.U, null},
//                {false, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, null, null, SpaceContext.DEFAULT, null, null, null},
//                {false, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, null, null, SpaceContext.DEFAULT, null, null, SQLError.FEATURE_EXISTS},
//                {false, true, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.ERROR, null, SpaceContext.DEFAULT, null, null, SQLError.VERSION_CONFLICT_ERROR},
//                {false, true, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.RETAIN, null, SpaceContext.DEFAULT, null, null, null},
//                {false, true, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.REPLACE, null, SpaceContext.DEFAULT, TableOperation.UPDATE, Operation.U, null},
//                {false, true, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, null, SpaceContext.DEFAULT, null, null, SQLError.ILLEGAL_ARGUMENT},
//                {false, true, true, false, null, null, true, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, OnMergeConflict.ERROR, SpaceContext.DEFAULT, null, null, SQLError.MERGE_CONFLICT_ERROR},
//                {false, true, true, false, null, null, true, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, OnMergeConflict.RETAIN, SpaceContext.DEFAULT, null, null, null},
//                {false, true, true, false, null, null, true, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, OnMergeConflict.REPLACE, SpaceContext.DEFAULT, TableOperation.UPDATE, Operation.U, null},
//                {true, false, false, null, null, null, null, UserIntent.WRITE, OnNotExists.CREATE, null, null, null, SpaceContext.DEFAULT, TableOperation.INSERT, Operation.I, null},
//                {true, false, false, null, null, null, null, UserIntent.WRITE, OnNotExists.ERROR, null, null, null, SpaceContext.DEFAULT, null, null, SQLError.FEATURE_NOT_EXISTS},
//                {true, false, false, null, null, null, null, UserIntent.WRITE, OnNotExists.RETAIN, null, null, null, SpaceContext.DEFAULT, null, null, null},
//                {true, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, null, null, SpaceContext.DEFAULT, TableOperation.DELETE, null, null},
//                {true, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, SpaceContext.DEFAULT, TableOperation.UPDATE, Operation.U, null},
//                {true, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, null, null, SpaceContext.DEFAULT, null, null, null},
//                {true, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, null, null, SpaceContext.DEFAULT, null, null, SQLError.FEATURE_EXISTS},
//                {true, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, null, null, SpaceContext.DEFAULT, TableOperation.DELETE, null, null},
//                {true, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, SpaceContext.DEFAULT, TableOperation.UPDATE, Operation.U, null},
//                {true, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, null, null, SpaceContext.DEFAULT, null, null, null},
//                {true, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, null, null, SpaceContext.DEFAULT, null, null, SQLError.FEATURE_EXISTS},
//                {true, false, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.ERROR, null, SpaceContext.DEFAULT, null, null, SQLError.MERGE_CONFLICT_ERROR},
//                {true, false, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.RETAIN, null, SpaceContext.DEFAULT, null, null, null},
//                {true, false, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.REPLACE, null, SpaceContext.DEFAULT, TableOperation.UPDATE, Operation.U, null},
//                {true, false, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, null, SpaceContext.DEFAULT, null, null, SQLError.ILLEGAL_ARGUMENT},
//                {true, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.CREATE, null, null, null, SpaceContext.DEFAULT, TableOperation.INSERT, Operation.I, null},
//                {true, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.ERROR, null, null, null, SpaceContext.DEFAULT, null, null, SQLError.FEATURE_NOT_EXISTS},
//                {true, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.RETAIN, null, null, null, SpaceContext.DEFAULT, null, null, null},
//                {true, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, null, null, SpaceContext.DEFAULT, TableOperation.DELETE, null, null},
//                {true, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, SpaceContext.DEFAULT, TableOperation.UPDATE, Operation.U, null},
//                {true, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, null, null, SpaceContext.DEFAULT, null, null, null},
//                {true, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, null, null, SpaceContext.DEFAULT, null, null, SQLError.FEATURE_EXISTS},
//                {true, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, null, null, SpaceContext.DEFAULT, TableOperation.DELETE, null, null},
//                {true, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, SpaceContext.DEFAULT, TableOperation.UPDATE, Operation.U, null},
//                {true, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, null, null, SpaceContext.DEFAULT, null, null, null},
//                {true, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, null, null, SpaceContext.DEFAULT, null, null, SQLError.FEATURE_EXISTS},
//                {true, true, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.ERROR, null, SpaceContext.DEFAULT, null, null, SQLError.VERSION_CONFLICT_ERROR},
//                {true, true, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.RETAIN, null, SpaceContext.DEFAULT, null, null, null},
//                {true, true, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.REPLACE, null, SpaceContext.DEFAULT, TableOperation.UPDATE, Operation.U, null},
//                {true, true, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, null, SpaceContext.DEFAULT, null, null, SQLError.ILLEGAL_ARGUMENT},
//                {true, true, true, false, null, null, true, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, OnMergeConflict.ERROR, SpaceContext.DEFAULT, null, null, SQLError.MERGE_CONFLICT_ERROR},
//                {true, true, true, false, null, null, true, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, OnMergeConflict.RETAIN, SpaceContext.DEFAULT, null, null, null},
//                {true, true, true, false, null, null, true, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, OnMergeConflict.REPLACE, SpaceContext.DEFAULT, TableOperation.UPDATE, Operation.U, null}
        });
    }
    
    @Test
    public void testFeatureWriterTextExecutor() throws Exception {
        //TODO: Check UserIntent

        writeFeature(simpleFeature(), DEFAULT_AUTHOR, onExists, onNotExists,
                onVersionConflict, onMergeConflict, false, spaceContext, history, expectedSQLError);

        if(onExists != null && onExists.equals(OnExists.RETAIN)
                || onVersionConflict != null && onVersionConflict.equals(OnVersionConflict.RETAIN)
                || onMergeConflict != null && onMergeConflict.equals(onMergeConflict.RETAIN)){
            //On RETAIN Strategy (feature exists) we expect no write and the existence of the old feature
            checkExistingFeature(expectedFeature, expectedVersion, expectedNextVersion, expectedFeatureOperation, DEFAULT_AUTHOR);
        }else if(onNotExists != null && onNotExists.equals(OnNotExists.RETAIN)){
            //On RETAIN Strategy (feature not exists) we expect no write
            checkNotExistingFeature(simpleFeature().getId());
        }
    }
}
