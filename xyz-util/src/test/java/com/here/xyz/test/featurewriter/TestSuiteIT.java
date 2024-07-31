package com.here.xyz.test.featurewriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
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

    private static Feature simpleFeature() throws JsonProcessingException {
        return XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{"firstName":"Alice","age":35}
            }
            """, Feature.class);
    }

    private static Feature simpleModifiedFeature() throws JsonProcessingException {
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

    private record Expectations(TableOperation tableOperation, Operation featureOperation,
                                Feature feature, long version, long nextVersion, String author,
                                SQLError sqlError) {
        public Expectations(SQLError sqlError){
            this(null, null, null, 0L, 0L, null, sqlError);
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testScenarios() throws JsonProcessingException {
        return Arrays.asList(new Object[][]{
                //** NO History + Feature not exists */
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
                    SpaceContext.EXTENSION,  //spaceContext

                    new Expectations(
                        TableOperation.INSERT,  //expectedTableOperation
                        Operation.I,  //expectedFeatureOperation
                        simpleFeature(),  //expectedFeature
                        1L,  //expectedVersion
                        Long.MAX_VALUE,  //expectedNextVersion
                        DEFAULT_AUTHOR,  //expectedAuthor
                        null  //expectedSQLError
                    )
                },
                { false, false, false, null, null, null, null, UserIntent.WRITE, OnNotExists.ERROR, null, null, null, SpaceContext.EXTENSION, new Expectations(SQLError.FEATURE_NOT_EXISTS) },
                { false, false, false, null, null, null, null, UserIntent.WRITE, OnNotExists.RETAIN, null, null, null, SpaceContext.EXTENSION, null },
                //** NO History + Feature exists */
                { false, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, null, null, SpaceContext.EXTENSION, null },
                { false, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, SpaceContext.EXTENSION,
                        new Expectations(
                                TableOperation.INSERT,  //expectedTableOperation
                                Operation.I,  //expectedFeatureOperation
                                simpleModifiedFeature(),  //expectedFeature
                                2L,  //expectedVersion
                                Long.MAX_VALUE,  //expectedNextVersion
                                UPDATE_AUTHOR,  //expectedAuthor
                                null  //expectedSQLError
                        )
                },
                { false, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, SpaceContext.EXTENSION,
                        new Expectations(
                                TableOperation.INSERT,  //expectedTableOperation
                                Operation.I,  //expectedFeatureOperation
                                simpleFeature(),  //expectedFeature
                                1L,  //expectedVersion
                                Long.MAX_VALUE,  //expectedNextVersion
                                DEFAULT_AUTHOR,  //expectedAuthor
                                null  //expectedSQLError
                        )
                },
                { false, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, null, null, SpaceContext.EXTENSION,
                        new Expectations(
                                TableOperation.INSERT,  //expectedTableOperation
                                Operation.I,  //expectedFeatureOperation
                                simpleFeature(),  //expectedFeature
                                1L,  //expectedVersion
                                Long.MAX_VALUE,  //expectedNextVersion
                                DEFAULT_AUTHOR,  //expectedAuthor
                                SQLError.FEATURE_EXISTS  //expectedSQLError
                        )
                },
                //** NO History + Feature exists + BaseVersionMatch */
                { false, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, null, null, SpaceContext.EXTENSION, null },
                { false, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, SpaceContext.EXTENSION,
                        new Expectations(
                                TableOperation.UPDATE,  //expectedTableOperation
                                Operation.U,  //expectedFeatureOperation
                                simpleModifiedFeature(),  //expectedFeature
                                2L,  //expectedVersion
                                Long.MAX_VALUE,  //expectedNextVersion
                                DEFAULT_AUTHOR,  //expectedAuthor
                                null  //expectedSQLError
                        )
                },
                { false, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, null, null, SpaceContext.EXTENSION,
                        new Expectations(
                                TableOperation.INSERT,  //expectedTableOperation
                                Operation.I,  //expectedFeatureOperation
                                simpleFeature(),  //expectedFeature
                                1L,  //expectedVersion
                                Long.MAX_VALUE,  //expectedNextVersion
                                DEFAULT_AUTHOR,  //expectedAuthor
                                SQLError.FEATURE_EXISTS  //expectedSQLError
                        )
                },
//                {false, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, null, null, SpaceContext.EXTENSION, null, null, SQLError.FEATURE_EXISTS},
//                {false, false, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.ERROR, null, SpaceContext.EXTENSION, null, null, SQLError.VERSION_CONFLICT_ERROR},
//                {false, false, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.RETAIN, null, SpaceContext.EXTENSION, null, null, null},
//                {false, false, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.REPLACE, null, SpaceContext.EXTENSION, TableOperation.UPDATE, Operation.U, null},
//                {false, false, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, null, SpaceContext.EXTENSION, null, null, SQLError.ILLEGAL_ARGUMENT},
//                {false, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.CREATE, null, null, null, SpaceContext.EXTENSION, TableOperation.INSERT, Operation.I, null},
//                {false, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.ERROR, null, null, null, SpaceContext.EXTENSION, null, null, SQLError.FEATURE_NOT_EXISTS},
//                {false, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.RETAIN, null, null, null, SpaceContext.EXTENSION, null, null, null},
//                {false, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, null, null, SpaceContext.EXTENSION, TableOperation.DELETE, null, null},
//                {false, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, SpaceContext.EXTENSION, TableOperation.UPDATE, Operation.U, null},
//                {false, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, null, null, SpaceContext.EXTENSION, null, null, null},
//                {false, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, null, null, SpaceContext.EXTENSION, null, null, SQLError.FEATURE_EXISTS},
//                {false, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, null, null, SpaceContext.EXTENSION, TableOperation.DELETE, null, null},
//                {false, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, SpaceContext.EXTENSION, TableOperation.UPDATE, Operation.U, null},
//                {false, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, null, null, SpaceContext.EXTENSION, null, null, null},
//                {false, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, null, null, SpaceContext.EXTENSION, null, null, SQLError.FEATURE_EXISTS},
//                {false, true, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.ERROR, null, SpaceContext.EXTENSION, null, null, SQLError.VERSION_CONFLICT_ERROR},
//                {false, true, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.RETAIN, null, SpaceContext.EXTENSION, null, null, null},
//                {false, true, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.REPLACE, null, SpaceContext.EXTENSION, TableOperation.UPDATE, Operation.U, null},
//                {false, true, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, null, SpaceContext.EXTENSION, null, null, SQLError.ILLEGAL_ARGUMENT},
//                {false, true, true, false, null, null, true, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, OnMergeConflict.ERROR, SpaceContext.EXTENSION, null, null, SQLError.MERGE_CONFLICT_ERROR},
//                {false, true, true, false, null, null, true, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, OnMergeConflict.RETAIN, SpaceContext.EXTENSION, null, null, null},
//                {false, true, true, false, null, null, true, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, OnMergeConflict.REPLACE, SpaceContext.EXTENSION, TableOperation.UPDATE, Operation.U, null},
//                {true, false, false, null, null, null, null, UserIntent.WRITE, OnNotExists.CREATE, null, null, null, SpaceContext.EXTENSION, TableOperation.INSERT, Operation.I, null},
//                {true, false, false, null, null, null, null, UserIntent.WRITE, OnNotExists.ERROR, null, null, null, SpaceContext.EXTENSION, null, null, SQLError.FEATURE_NOT_EXISTS},
//                {true, false, false, null, null, null, null, UserIntent.WRITE, OnNotExists.RETAIN, null, null, null, SpaceContext.EXTENSION, null, null, null},
//                {true, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, null, null, SpaceContext.EXTENSION, TableOperation.DELETE, null, null},
//                {true, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, SpaceContext.EXTENSION, TableOperation.UPDATE, Operation.U, null},
//                {true, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, null, null, SpaceContext.EXTENSION, null, null, null},
//                {true, false, true, null, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, null, null, SpaceContext.EXTENSION, null, null, SQLError.FEATURE_EXISTS},
//                {true, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, null, null, SpaceContext.EXTENSION, TableOperation.DELETE, null, null},
//                {true, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, SpaceContext.EXTENSION, TableOperation.UPDATE, Operation.U, null},
//                {true, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, null, null, SpaceContext.EXTENSION, null, null, null},
//                {true, false, true, true, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, null, null, SpaceContext.EXTENSION, null, null, SQLError.FEATURE_EXISTS},
//                {true, false, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.ERROR, null, SpaceContext.EXTENSION, null, null, SQLError.MERGE_CONFLICT_ERROR},
//                {true, false, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.RETAIN, null, SpaceContext.EXTENSION, null, null, null},
//                {true, false, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.REPLACE, null, SpaceContext.EXTENSION, TableOperation.UPDATE, Operation.U, null},
//                {true, false, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, null, SpaceContext.EXTENSION, null, null, SQLError.ILLEGAL_ARGUMENT},
//                {true, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.CREATE, null, null, null, SpaceContext.EXTENSION, TableOperation.INSERT, Operation.I, null},
//                {true, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.ERROR, null, null, null, SpaceContext.EXTENSION, null, null, SQLError.FEATURE_NOT_EXISTS},
//                {true, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.RETAIN, null, null, null, SpaceContext.EXTENSION, null, null, null},
//                {true, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, null, null, SpaceContext.EXTENSION, TableOperation.DELETE, null, null},
//                {true, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, SpaceContext.EXTENSION, TableOperation.UPDATE, Operation.U, null},
//                {true, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, null, null, SpaceContext.EXTENSION, null, null, null},
//                {true, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, null, null, SpaceContext.EXTENSION, null, null, SQLError.FEATURE_EXISTS},
//                {true, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, null, null, SpaceContext.EXTENSION, TableOperation.DELETE, null, null},
//                {true, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, SpaceContext.EXTENSION, TableOperation.UPDATE, Operation.U, null},
//                {true, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, null, null, SpaceContext.EXTENSION, null, null, null},
//                {true, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, null, null, SpaceContext.EXTENSION, null, null, SQLError.FEATURE_EXISTS},
//                {true, true, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.ERROR, null, SpaceContext.EXTENSION, null, null, SQLError.VERSION_CONFLICT_ERROR},
//                {true, true, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.RETAIN, null, SpaceContext.EXTENSION, null, null, null},
//                {true, true, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.REPLACE, null, SpaceContext.EXTENSION, TableOperation.UPDATE, Operation.U, null},
//                {true, true, true, false, null, true, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, null, SpaceContext.EXTENSION, null, null, SQLError.ILLEGAL_ARGUMENT},
//                {true, true, true, false, null, null, true, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, OnMergeConflict.ERROR, SpaceContext.EXTENSION, null, null, SQLError.MERGE_CONFLICT_ERROR},
//                {true, true, true, false, null, null, true, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, OnMergeConflict.RETAIN, SpaceContext.EXTENSION, null, null, null},
//                {true, true, true, false, null, null, true, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, OnMergeConflict.REPLACE, SpaceContext.EXTENSION, TableOperation.UPDATE, Operation.U, null}
        });
    }
    
    @Test
    public void testFeatureWriterTextExecutor() throws Exception {
        //TODO: Check UserIntent

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
            && onExists != null && onExists.equals(OnExists.DELETE)){
            //On RETAIN Strategy (feature not exists) we expect no write
            checkNotExistingFeature(simpleFeature().getId());
        }
    }
}
