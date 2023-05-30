package com.here.mapcreator.ext.naksha;

import java.io.IOException;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

public class PsqlClientTest {
  @Test
  @EnabledIf("isAllTestEnvVarsSet")
  public void testInit() {

    final PsqlConfig config = new PsqlConfigBuilder()
        .withHost(System.getenv("NAKSHA_DB_HOST"))
        .withUser(System.getenv("NAKSHA_DB_USERNAME"))
        .withPassword(System.getenv("NAKSHA_DB_PASSWORD"))
        .withSchema("Naksha-default")
        .withAppName("test-app")
        .withDb(System.getenv("NAKSHA_DB"))
        .build();
    final PsqlDataSource ds = new PsqlDataSource(config);
    final PsqlClient<PsqlDataSource> client = new PsqlClient<>(ds, 0L);
    try {
      client.init();
    } catch (SQLException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean isAllTestEnvVarsSet() {
    return System.getenv("NAKSHA_DB_HOST") != null && System.getenv("NAKSHA_DB_USERNAME") != null
        && System.getenv("NAKSHA_DB_PASSWORD") != null && System.getenv("NAKSHA_DB") != null;
  }
}
