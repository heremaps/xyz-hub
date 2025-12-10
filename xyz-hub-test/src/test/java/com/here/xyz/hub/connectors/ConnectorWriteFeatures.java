package com.here.xyz.hub.connectors;

import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
import com.here.xyz.benchmarks.tools.PerformanceTestHelper;
import com.here.xyz.connectors.StorageConnector;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.NLConnector;
import com.here.xyz.psql.PSQLXyzConnector;
import com.here.xyz.psql.PSQLXyzNLConnector;
import com.here.xyz.util.Random;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.db.datasource.PooledDataSources;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.here.xyz.events.ModifySpaceEvent.Operation.CREATE;
import static org.junit.Assert.assertTrue;

public class ConnectorWriteFeatures {
  private static enum TARGET_CONNECTOR { PSQL_CONNECTOR, PSQL_NL_CONNECTOR, NL_CONNECTOR};
  private static final DatabaseSettings dbSettings =
          PerformanceTestHelper.createDBSettings("localhost", "postgres", "postgres","password", 40);
  private static StorageConnector PSQL_CONNECTOR;
  private static StorageConnector NL_CONNECTOR;
  private static StorageConnector PSQL_NL_CONNECTOR;
  private static String spaceName =  ConnectorWriteFeatures.class.getSimpleName() +"."+Random.randomAlpha(6);
  private static StorageConnector connector;

  private static Map<String, Boolean> spaceSearchableProperties = Map.of(
        "foo1", true,  //equals to: "$foo1:foo1:scalar"
        "foo2.nested", true, //equals to: "$foo2.nested:foo2.nested:scalar"
        "foo3.nested.arr::array", true, //equals to: "$foo3.nested:foo3.nested:array"
        //---- new -----/
        "$alias1:[$.properties.street.fc]::scalar", true,
        "$alias2:[$.properties.names[*].lang]::array", true
    );

  private static Map<String, String> eventSearchableProperties = Map.of(
        "foo1", "$.properties.foo1::scalar",
        "foo2.nested","$.properties.foo2.nested",
        "foo3.nested.arr", "$.properties.foo3.nested.arr",
        "alias1", "$.properties.street.fc",
        "alias2", "$.properties.names[*].lang::array"
    );


  @BeforeAll
  public static void setup() {

  try {
    connector = loadConnector(TARGET_CONNECTOR.PSQL_NL_CONNECTOR);

    //createSpace
    ModifySpaceEvent modifySpaceEvent = new ModifySpaceEvent()
            .withSpaceDefinition(new Space()
                    .withSearchableProperties(spaceSearchableProperties)
                    .withId(spaceName)
                    .withVersionsToKeep(1000)
            )
            .withSpace(spaceName)
            .withOperation(CREATE);

    Typed xyzResponse = connector.handleEvent(modifySpaceEvent);

            // setup logic
    } catch (Exception e) {
        assertTrue("Skipping test because setup failed: " + e.getMessage(), false);
    }

  }

  @AfterAll
  public static void tearDown() {
   try {
    PerformanceTestHelper.deleteSpace(connector, spaceName);
  } catch (Exception e) {
    assertTrue("Skipping test because tearDown failed: " + e.getMessage(), false);
  }
  }

  private static String testFc = """
  {
   "type": "FeatureCollection",
   "features": [
    {
      "type": "Feature",
      "id": "#TestID#",
      "geometry": { "type": "Point","coordinates": [-2.96084734567,53.43082834567] },
      "properties": {
        "foo1": "foo1-value",
        "foo2": { "nested" : "foo2-nested-value" },
        "foo3": { "nested" : { "arr" : ["foo3-nested-value1","foo3-nested-value2","foo3-nested-value3"] } },
        "location": { "lat": 40.785091, "lon": -73.968285 },
        "tags": ["park", "NYC", "landmark"],
        "street": { "fc": 5 },
        "names": [ { "lang": "en", "value": "Tree" },
                   { "lang": "de", "value": "Baum" },
                   { "lang": "fr", "value": "Arbre" }
                 ],
        "str1": "str1",
        "str2": "str2"
      }
    }
   ]
 }
 """,
 resultSearchable = """
 {"foo1": "foo1-value",
  "alias1": 5,
  "alias2": "en",
  "foo2.nested": "foo2-nested-value",
  "foo3.nested.arr": ["foo3-nested-value1", "foo3-nested-value2", "foo3-nested-value3"]
  }
 """;

  private static Stream<Arguments> provideParameters() {
    return Stream.of(
        Arguments.of(1),
        Arguments.of(1000)
    );
  }

  @ParameterizedTest
  @MethodSource("provideParameters")
  public void testWriteFeaturesWithSearchableProperties(int v2k) throws Exception {

  String fid = "TestID-v2k-" + v2k;

  FeatureCollection fc = XyzSerializable.deserialize(testFc.replace("#TestID#", fid), FeatureCollection.class);

  WriteFeaturesEvent wfe = new WriteFeaturesEvent()
            .withSpace(spaceName)
            .withVersionsToKeep(v2k)
            .withSearchableProperties(eventSearchableProperties)
            .withModifications(Set.of(new WriteFeaturesEvent.Modification()
                                           .withUpdateStrategy(UpdateStrategy.DEFAULT_UPDATE_STRATEGY)
                                           .withFeatureData(fc)));

   Typed xyzResponse = connector.handleEvent(wfe);

  // check for correct values in searchable column
  try (DataSourceProvider dsp = new PooledDataSources(dbSettings)) {

      String returnedValue = new SQLQuery("SELECT searchable::text from ${TheTable} where id = #{fid}")
                                    .withVariable("TheTable", spaceName)
                                    .withNamedParameter("fid", fid)
          .run(dsp, rs -> rs.next() ? rs.getString(1) : null);

      Map<String, Object> searchable = XyzSerializable.deserialize(returnedValue, Map.class);

      assertTrue("searchable foo1 failed" ,
                 searchable.containsKey("foo1") && searchable.get("foo1").equals("foo1-value"));
      assertTrue("searchable alias1 failed" ,
                 searchable.containsKey("alias1") && (int) searchable.get("alias1") == 5);

      assertTrue("searchable alias2 failed" ,
                    searchable.containsKey("alias2")
                 && ((List<?>) searchable.get("alias2")).size() == 3
                 && "en".equals( ((List<?>) searchable.get("alias2")).get(0) )
                 && "de".equals( ((List<?>) searchable.get("alias2")).get(1) )
                 && "fr".equals( ((List<?>) searchable.get("alias2")).get(2) )
                );
      assertTrue("searchable foo2.nested failed" ,
                 searchable.containsKey("foo2.nested") && searchable.get("foo2.nested").equals("foo2-nested-value"));
      assertTrue("searchable foo3.nested.arr failed" ,
                 searchable.containsKey("foo3.nested.arr") && ((List<?>) searchable.get("foo3.nested.arr")).size() == 3 );
    }

  }

  private static StorageConnector loadConnector(TARGET_CONNECTOR targetConnector) throws Exception {
    if(PSQL_CONNECTOR != null)
      return PSQL_CONNECTOR;
    if(NL_CONNECTOR != null)
      return NL_CONNECTOR;
    if(PSQL_NL_CONNECTOR != null)
      return PSQL_NL_CONNECTOR;

    return switch (targetConnector){
      case PSQL_CONNECTOR -> PerformanceTestHelper.initConnector("TEST_" + targetConnector, new PSQLXyzConnector(), dbSettings);
      case NL_CONNECTOR -> PerformanceTestHelper.initConnector("TEST_" + targetConnector, new NLConnector(), dbSettings).withSeedingMode(false);
      case PSQL_NL_CONNECTOR ->PerformanceTestHelper.initConnector("TEST_" + targetConnector, new PSQLXyzNLConnector(), dbSettings);
    };
  }
}
