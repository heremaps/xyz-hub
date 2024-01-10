package com.here.naksha.app.init.context;

import static com.here.naksha.app.service.NakshaApp.newInstance;

import com.here.naksha.app.init.PostgresContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerTestContext extends TestContext {

  private static final Logger log = LoggerFactory.getLogger(ContainerTestContext.class);

  private static final String CONFIG_ID = "test-config-with-extensions";

  private final PostgresContainer postgresContainer;

  public ContainerTestContext() {
    this(PostgresContainer.startedPostgresContainer());
  }

  private ContainerTestContext(final PostgresContainer postgresContainer) {
    super(() -> newInstance(CONFIG_ID, postgresContainer.getJdbcUrl()));
    this.postgresContainer = postgresContainer;
  }

  @Override
  void teardownStorage() {
    super.teardownStorage();
    log.info("Stopping postgres container");
    postgresContainer.stop();
  }
}
