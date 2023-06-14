package com.here.naksha.psql;

import com.here.mapcreator.ext.naksha.PsqlClient;
import com.here.mapcreator.ext.naksha.PsqlConfig;
import com.here.mapcreator.ext.naksha.PsqlConfigBuilder;
import com.here.mapcreator.ext.naksha.PsqlDataSource;
import com.here.mapcreator.ext.naksha.PsqlTransaction;
import com.here.xyz.exceptions.ParameterError;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

public class PsqlClientTest {
  // Example: TEST_ADMIN_DB=jdbc:postgresql://localhost/postgres?user=postgres&password=password&schema=test

  @Test
  @EnabledIf("isAllTestEnvVarsSet")
  public void testInit() throws URISyntaxException, ParameterError {
    final PsqlConfig config = new PsqlConfigBuilder()
        .withAppName("Naksha-Psql-Test")
        .parseUrl(System.getenv("TEST_ADMIN_DB"))
        .build();
    final PsqlClient<PsqlDataSource> client = PsqlClient.create(config,0);
    try {
      client.init();
      try (final PsqlTransaction<PsqlDataSource> transaction = client.startMutation()) {
        transaction.createCollection("testID", true);
        //TODO port over UMapView and related classes from wikvaya
        //transaction.commit();
      }
    } catch (SQLException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean isAllTestEnvVarsSet() {
    return System.getenv("TEST_ADMIN_DB") != null && System.getenv("TEST_ADMIN_DB").length() > 0;
  }
}
