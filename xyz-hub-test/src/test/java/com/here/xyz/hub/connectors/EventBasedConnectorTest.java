package com.here.xyz.hub.connectors;

import com.here.xyz.connectors.StorageConnector;
import com.here.xyz.psql.NLConnector;
import com.here.xyz.psql.PSQLXyzConnector;
import com.here.xyz.psql.PSQLXyzNLConnector;
import com.here.xyz.util.db.datasource.DatabaseSettings;

import static com.here.xyz.benchmarks.tools.PerformanceTestHelper.createDBSettings;
import static com.here.xyz.benchmarks.tools.PerformanceTestHelper.initConnector;

public class EventBasedConnectorTest {
  private static final DatabaseSettings dbSettings =
          createDBSettings("localhost", "postgres", "postgres","password", 40);

  protected static float xmin = 7.0f, ymin = 50.0f, xmax = 7.1f, ymax = 50.1f;

  protected enum TARGET_CONNECTOR { PSQL_CONNECTOR, NL_CONNECTOR, PSQL_NL_CONNECTOR};

  protected StorageConnector PSQL_CONNECTOR;
  protected StorageConnector PSQL_NL_CONNECTOR;
  protected StorageConnector NL_CONNECTOR;
  
  protected StorageConnector loadConnector(TARGET_CONNECTOR targetConnector) throws Exception {
    if(PSQL_CONNECTOR != null)
      return PSQL_CONNECTOR;
    if(NL_CONNECTOR != null)
      return NL_CONNECTOR;
    if(PSQL_NL_CONNECTOR != null)
      return PSQL_NL_CONNECTOR;

    return switch (targetConnector){
      case PSQL_CONNECTOR -> initConnector("TEST_" + targetConnector, new PSQLXyzConnector(), dbSettings);
      case PSQL_NL_CONNECTOR -> initConnector("TEST_" + targetConnector, new PSQLXyzNLConnector(), dbSettings);
      case NL_CONNECTOR -> initConnector("TEST_" + targetConnector, new NLConnector(), dbSettings)
              .withSeedingMode(false);
    };
  }
}
