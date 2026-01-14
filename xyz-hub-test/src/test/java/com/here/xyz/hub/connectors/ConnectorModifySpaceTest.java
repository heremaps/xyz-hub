package com.here.xyz.hub.connectors;

import com.here.xyz.Typed;
import com.here.xyz.benchmarks.tools.PerformanceTestHelper;
import com.here.xyz.connectors.StorageConnector;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.NLConnector;
import com.here.xyz.psql.PSQLXyzConnector;
import com.here.xyz.psql.PSQLXyzNLConnector;
import com.here.xyz.util.Random;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import org.junit.Test;

import java.util.Map;

import static com.here.xyz.events.ModifySpaceEvent.Operation.CREATE;

public class ConnectorModifySpaceTest {
  private enum TARGET_CONNECTOR { PSQL_CONNECTOR, PSQL_NL_CONNECTOR, NL_CONNECTOR};
  private static final DatabaseSettings dbSettings =
          PerformanceTestHelper.createDBSettings("localhost", "postgres", "postgres","password", 40);
  private StorageConnector PSQL_CONNECTOR;
  private StorageConnector PSQL_NL_CONNECTOR;
  //POC connector
  private StorageConnector NL_CONNECTOR;

  @Test
  public void testCreateSpaceWithSearchableProperties() throws Exception {
    StorageConnector connector = loadConnector(TARGET_CONNECTOR.PSQL_NL_CONNECTOR);

    String spaceName =  this.getClass().getSimpleName() +"."+Random.randomAlpha(6);

    Map<String, Boolean> searchableProperties = Map.of(
            "foo1", true,
            "foo2.nested", true,
            "foo3.nested.array::array", true
    );

    //createSpace
    ModifySpaceEvent modifySpaceEvent = new ModifySpaceEvent()
            .withSpaceDefinition(new Space()
                    .withSearchableProperties(searchableProperties)
                    .withId(spaceName)
            )
            .withSpace(spaceName)
            .withOperation(CREATE);

    Typed xyzResponse = connector.handleEvent(modifySpaceEvent);
    PerformanceTestHelper.deleteSpace(connector, spaceName);
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
      case PSQL_NL_CONNECTOR -> PerformanceTestHelper.initConnector("TEST_" + targetConnector, new PSQLXyzNLConnector(), dbSettings);
      case NL_CONNECTOR -> PerformanceTestHelper.initConnector("TEST_" + targetConnector, new NLConnector(), dbSettings);
    };
  }
}
