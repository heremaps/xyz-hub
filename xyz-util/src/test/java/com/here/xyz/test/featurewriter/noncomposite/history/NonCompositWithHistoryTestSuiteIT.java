package com.here.xyz.test.featurewriter.noncomposite.history;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.test.featurewriter.TestSuiteIT;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class NonCompositWithHistoryTestSuiteIT extends TestSuiteIT {

    public NonCompositWithHistoryTestSuiteIT(String testName, boolean composite, boolean history, boolean featureExists,
                                            Boolean baseVersionMatch, Boolean conflictingAttributes, Boolean featureExistsInSuper, Boolean featureExistsInExtension,
                                            UserIntent userIntent, OnNotExists onNotExists, OnExists onExists, OnVersionConflict onVersionConflict,
                                            OnMergeConflict onMergeConflict, SpaceContext spaceContext, TestSuiteIT.Expectations expectations) {
        super(testName, composite, history, featureExists, baseVersionMatch, conflictingAttributes, featureExistsInSuper, featureExistsInExtension,
                userIntent, onNotExists, onExists, onVersionConflict, onMergeConflict, spaceContext, expectations);
    }

    @Parameterized.Parameters(name = "Test Number #{0}")
    public static Collection<Object[]> testScenarios() throws JsonProcessingException {
        return Arrays.asList(new Object[][]{
            /** Feature NOT exists */
           {  "0", false, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.CREATE, null, null, null, null,
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
            { "1", false, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.ERROR, null, null, null, null,  new Expectations(SQLError.FEATURE_NOT_EXISTS) },
            { "2", false, true, false, null, null, null, null, UserIntent.WRITE, OnNotExists.RETAIN, null, null, null, null, null },

       /** Feature exists + no ConflictHandling */
            { "3", false, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, null, null, null, null },
            { "4", false, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, null, null, null,
                    new Expectations(
                            TableOperation.UPDATE,  //expectedTableOperation
                            Operation.U,  //expectedFeatureOperation
                            simple1thModificatedFeature(),  //expectedFeature
                            2L,  //expectedVersion
                            Long.MAX_VALUE,  //expectedNextVersion
                            UPDATE_AUTHOR,  //expectedAuthor
                            null  //expectedSQLError
                    )
            },
            { "5", false, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, null, null, null,
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
            { "6", false, true, true, null, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, null, null, null,
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
            /** Feature exists and got updated. Third write will have a Baseversion MATCH */
            { "7", false, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, OnVersionConflict.REPLACE, null, null,
                    new Expectations(
                            TableOperation.INSERT,  //expectedTableOperation
                            Operation.D,  //expectedFeatureOperation
                            simple1thModificatedFeature(),  //expectedFeature
                           //TODO: Check if version=3 is correct!
                            3L,  //expectedVersion
                            Long.MAX_VALUE,  //expectedNextVersion
                            DEFAULT_AUTHOR,  //expectedAuthor
                            null  //expectedSQLError
                    )
            },
            { "8", false, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.REPLACE, null, null,
                    new Expectations(
                            TableOperation.INSERT,  //expectedTableOperation
                            Operation.U,  //expectedFeatureOperation
                            simple2thModificatedFeature(3L,false),  //expectedFeature
                            3L,  //expectedVersion
                            Long.MAX_VALUE,  //expectedNextVersion
                            UPDATE_AUTHOR,  //expectedAuthor
                            null  //expectedSQLError
                    )
            },
            { "9", false, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, OnVersionConflict.REPLACE, null, null,
                    new Expectations(
                            TableOperation.INSERT,  //expectedTableOperation
                            Operation.U,  //expectedFeatureOperation
                            simple1thModificatedFeature(),  //expectedFeature
                            2L,  //expectedVersion
                            Long.MAX_VALUE,  //expectedNextVersion
                            DEFAULT_AUTHOR,  //expectedAuthor
                            null  //expectedSQLError
                    )
            },
            { "10", false, true, true, true, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, OnVersionConflict.REPLACE, null, null,
                    new Expectations(
                            TableOperation.INSERT,  //expectedTableOperation
                            Operation.U,  //expectedFeatureOperation
                            simple1thModificatedFeature(),  //expectedFeature
                            2L,  //expectedVersion
                            Long.MAX_VALUE,  //expectedNextVersion
                            DEFAULT_AUTHOR,  //expectedAuthor
                            SQLError.FEATURE_EXISTS  //expectedSQLError
                    )
            },

            /** Feature exists and got updated. Third write will have a Baseversion MISSMATCH. With ConflictHandling -> ERROR, RETAIN  */
            { "11", false, true, true, false, null, null, null, UserIntent.WRITE, null, null, OnVersionConflict.ERROR, null, null,
                    new Expectations(
                            TableOperation.INSERT,  //expectedTableOperation
                            Operation.U,  //expectedFeatureOperation
                            simple1thModificatedFeature(),  //expectedFeature
                            2L,  //expectedVersion
                            Long.MAX_VALUE,  //expectedNextVersion
                            DEFAULT_AUTHOR,  //expectedAuthor
                            SQLError.VERSION_CONFLICT_ERROR  //expectedSQLError
                    )
            },
            { "12", false, true, true, false, null, null, null, UserIntent.WRITE, null, null, OnVersionConflict.RETAIN, null, null,
                    new Expectations(
                            TableOperation.INSERT,  //expectedTableOperation
                            Operation.U,  //expectedFeatureOperation
                            simple1thModificatedFeature(),  //expectedFeature
                            2L,  //expectedVersion
                            Long.MAX_VALUE,  //expectedNextVersion
                            DEFAULT_AUTHOR,  //expectedAuthor
                            null  //expectedSQLError
                    )
            },
            /**  Feature exists and got updated. Third write will have a Baseversion MISSMATCH. With ConflictHandling -> REPLACE */
            { "13", false, true, true, false, null, null, null, UserIntent.WRITE, null, OnExists.DELETE, OnVersionConflict.REPLACE, null, null,
                    new Expectations(
                            TableOperation.INSERT,  //expectedTableOperation
                            Operation.D,  //expectedFeatureOperation
                            simple1thModificatedFeature(),  //expectedFeature
                            3L,  //expectedVersion
                            Long.MAX_VALUE,  //expectedNextVersion
                            DEFAULT_AUTHOR,  //expectedAuthor
                            null  //expectedSQLError
                    )
            },
            { "14", false, true, true, false, null, null, null, UserIntent.WRITE, null, OnExists.REPLACE, OnVersionConflict.REPLACE, null, null,
                    new Expectations(
                            TableOperation.INSERT,  //expectedTableOperation
                            Operation.U,  //expectedFeatureOperation
                            simple2thModificatedFeature(3L, false),  //expectedFeature
                            3L,  //expectedVersion
                            Long.MAX_VALUE,  //expectedNextVersion
                            UPDATE_AUTHOR,  //expectedAuthor
                            null  //expectedSQLError
                    )
            },
            { "15", false, true, true, false, null, null, null, UserIntent.WRITE, null, OnExists.RETAIN, OnVersionConflict.REPLACE, null, null,
                    new Expectations(
                            TableOperation.INSERT,  //expectedTableOperation
                            Operation.U,  //expectedFeatureOperation
                            simple1thModificatedFeature(),  //expectedFeature
                            2L,  //expectedVersion
                            Long.MAX_VALUE,  //expectedNextVersion
                            DEFAULT_AUTHOR,  //expectedAuthor
                            null  //expectedSQLError
                    )
            },
            { "16", false, true, true, false, null, null, null, UserIntent.WRITE, null, OnExists.ERROR, OnVersionConflict.REPLACE, null, null,
                    new Expectations(
                            TableOperation.INSERT,  //expectedTableOperation
                            Operation.U,  //expectedFeatureOperation
                            simple1thModificatedFeature(),  //expectedFeature
                            2L,  //expectedVersion
                            Long.MAX_VALUE,  //expectedNextVersion
                            DEFAULT_AUTHOR,  //expectedAuthor
                            SQLError.FEATURE_EXISTS  //expectedSQLError
                    )
            },
            /** Feature exists and got updated. Third write will have a Baseversion MISSMATCH. With ConflictHandling -> MERGE (NoConflicting Changes) */
            { "17", false, true, true, false, false, null, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, null, null,
                    new Expectations(
                            TableOperation.INSERT,  //expectedTableOperation
                            Operation.U,  //expectedFeatureOperation
                            XyzSerializable.deserialize("""
                                    { "type":"Feature",
                                      "id":"id1",
                                      "geometry":{"type":"Point","coordinates":[8.0,50.0]},
                                      "properties":{"age":"32", "lastName" : "wonder"}
                                    }
                                    """, Feature.class),  //expectedFeature
                            3L,  //expectedVersion
                            Long.MAX_VALUE,  //expectedNextVersion
                            DEFAULT_AUTHOR,  //expectedAuthor
                            null  //expectedSQLError
                    )
            },

            /** Feature exists + With ConflictHandling + Baseversion MISSMATCH -> MERGE Conflicting*/
            { "18", false, true, true, false, true, null, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, OnMergeConflict.ERROR, null,
                    new Expectations(
                            TableOperation.INSERT,  //expectedTableOperation
                            Operation.U,  //expectedFeatureOperation
                            simple1thModificatedFeature(),  //expectedFeature
                            2L,  //expectedVersion
                            Long.MAX_VALUE,  //expectedNextVersion
                            DEFAULT_AUTHOR,  //expectedAuthor
                            SQLError.MERGE_CONFLICT_ERROR  //expectedSQLError
                    )
            },
            { "19", false, true, true, false, true, null, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, OnMergeConflict.RETAIN, null,
                    new Expectations(
                            TableOperation.INSERT,  //expectedTableOperation
                            Operation.U,  //expectedFeatureOperation
                            simple1thModificatedFeature(),  //expectedFeature
                            2L,  //expectedVersion
                            Long.MAX_VALUE,  //expectedNextVersion
                            DEFAULT_AUTHOR,  //expectedAuthor
                            null  //expectedSQLError
                    )
            },
            { "20", false, true, true, false, true, null, null, UserIntent.WRITE, null, null, OnVersionConflict.MERGE, OnMergeConflict.REPLACE, null,
                    new Expectations(
                            TableOperation.INSERT,  //expectedTableOperation
                            Operation.U,  //expectedFeatureOperation
                            simple2thModificatedFeature(3L, true),  //expectedFeature
                            3L,  //expectedVersion
                            Long.MAX_VALUE,  //expectedNextVersion
                            DEFAULT_AUTHOR,  //expectedAuthor
                            null  //expectedSQLError
                    )
            },
        });
    }

    @Test
    public void start() throws Exception {
        featureWriterExecutor();
    }
}
