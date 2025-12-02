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
import com.here.xyz.util.db.datasource.DatabaseSettings;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static com.here.xyz.events.ModifySpaceEvent.Operation.CREATE;
import static org.junit.Assume.assumeTrue;

public class ConnectorWriteFeatures {
  private enum TARGET_CONNECTOR { PSQL_CONNECTOR, PSQL_NL_CONNECTOR, NL_CONNECTOR};
  private static final DatabaseSettings dbSettings =
          PerformanceTestHelper.createDBSettings("localhost", "postgres", "postgres","password", 40);
  private StorageConnector PSQL_CONNECTOR;
  private StorageConnector NL_CONNECTOR;
  private StorageConnector PSQL_NL_CONNECTOR;
  private String spaceName =  this.getClass().getSimpleName() +"."+Random.randomAlpha(6);
  private StorageConnector connector;

  private static Map<String, Boolean> spaceSearchableProperties = Map.of(
        "foo1", true,  //equals to: "$foo1:foo1:scalar"
        "foo2.nested", true, //equals to: "$foo2.nested:foo2.nested:scalar"
        "foo3.nested.arr::array", true, //equals to: "$foo3.nested:foo3.nested:array"
        //---- new -----/
        "$alias1:[$.properties.street.fc]::scalar", true,
        "$alias2:[$.properties.names[*].lang]::array", true
    );

  private static Map<String, String> eventSearchableProperties = Map.of(
        "foo1", "$.properties.foo1",
        "foo2.nested","$.properties.foo2.nested",
        "foo3.nested.arr", "$.properties.foo3.nested.arr",
        "alias1", "$.properties.street.fc",
        "alias2", "$.properties.names[*].lang"
    );


  @Before
  public void setup() {

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
        assumeTrue("Skipping test because setup failed: " + e.getMessage(), false);
    }

  }

  @After
  public void tearDown() {
   try {
    PerformanceTestHelper.deleteSpace(connector, spaceName);
  } catch (Exception e) {
    assumeTrue("Skipping test because tearDown failed: " + e.getMessage(), false);
  }
  }

  private static String testFc = """
  {
   "type": "FeatureCollection",
   "features": [
    {
      "type": "Feature",
      "id": "test-09",
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
 """;

  @Test
  public void testWriteFeaturesWithSearchableProperties() throws Exception {

  FeatureCollection fc = XyzSerializable.deserialize(testFc, FeatureCollection.class);

  WriteFeaturesEvent wfe = new WriteFeaturesEvent()
            .withSpace(spaceName)
            .withVersionsToKeep(1)
            .withSearchableProperties(eventSearchableProperties)
            .withModifications(Set.of(new WriteFeaturesEvent.Modification()
                                           .withUpdateStrategy(UpdateStrategy.DEFAULT_UPDATE_STRATEGY)
                                           .withFeatureData(fc)));

   Typed xyzResponse = connector.handleEvent(wfe);

  }

  private StorageConnector loadConnector(TARGET_CONNECTOR targetConnector) throws Exception {
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
