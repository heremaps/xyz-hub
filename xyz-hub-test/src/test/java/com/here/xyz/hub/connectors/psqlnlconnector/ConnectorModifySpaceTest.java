package com.here.xyz.hub.connectors.psqlnlconnector;

import com.here.xyz.benchmarks.tools.PerformanceTestHelper;
import com.here.xyz.connectors.StorageConnector;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.hub.connectors.EventBasedConnectorTest;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.Random;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static com.here.xyz.events.ModifySpaceEvent.Operation.CREATE;

public class ConnectorModifySpaceTest extends EventBasedConnectorTest {

  @Test
  public void testCreateSpaceWithSearchableProperties() throws Exception {
    StorageConnector connector = loadConnector(TARGET_CONNECTOR.PSQL_NL_CONNECTOR);

    String spaceName =  this.getClass().getSimpleName() +"."+ Random.randomAlpha(6);

    //TODO: Test also legacy searchable properties
    Map<String, Boolean> searchableProperties = Map.of(
//            "f.root", true,
//            "foo1.nested", true,
            "$alias1:[$.properties.refQuad]::scalar", true,
            "$alias2:[$.properties.globalVersion]::scalar", true,
            "$alias3:[$.properties.names[*].lang]::array", true
    );

    /* create space */
    ModifySpaceEvent modifySpaceEvent = new ModifySpaceEvent()
            .withSpaceDefinition(new Space()
                    .withSearchableProperties(searchableProperties)
                    .withId(spaceName)
                    .withVersionsToKeep(1000)
            )
            .withSpace(spaceName)
            .withOperation(CREATE);

    connector.handleEvent(modifySpaceEvent);

    /* write features */
    FeatureCollection fc = PerformanceTestHelper.generateRandomFeatureCollection(10001, xmin, ymin, xmax, ymax, 100, false);

    WriteFeaturesEvent writeFeaturesEvent = new WriteFeaturesEvent()
            .withModifications(Set.of(
                    new WriteFeaturesEvent.Modification()
                            .withFeatureData(fc)
                            .withUpdateStrategy(UpdateStrategy.DEFAULT_UPDATE_STRATEGY)
            ))
            .withSearchableProperties(Space.toExtractableSearchProperties(
                    new com.here.xyz.hub.connectors.models.Space().withSearchableProperties(searchableProperties)))
            .withSpace(spaceName)
            .withResponseDataExpected(true);
    FeatureCollection f2 = (FeatureCollection) connector.handleEvent(writeFeaturesEvent);
    System.out.println(f2.getFeatures().size());

    //TODO: add tests
    /* clean test resources */
    PerformanceTestHelper.deleteSpace(connector, spaceName);
  }
}
