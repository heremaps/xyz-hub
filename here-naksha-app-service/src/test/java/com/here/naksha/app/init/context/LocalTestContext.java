package com.here.naksha.app.init.context;

import static com.here.naksha.app.service.NakshaApp.newInstance;

import com.here.naksha.app.init.TestPsqlStorageConfigs;
import com.here.naksha.lib.psql.PsqlStorage;
import com.here.naksha.lib.psql.PsqlStorageConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalTestContext extends TestContext {

  private static final Logger log = LoggerFactory.getLogger(LocalTestContext.class);

  private static final String CONFIG_ID = "test-config";
  private static final PsqlStorageConfig STORAGE_CONFIG = TestPsqlStorageConfigs.dataDbConfig;

  public LocalTestContext() {
    super(() -> newInstance(CONFIG_ID, STORAGE_CONFIG.url()));
  }

  @Override
  void setupStorage() {
    super.setupStorage();
    log.info("Cleaning up schema for url: {}", STORAGE_CONFIG.url());
    if (!STORAGE_CONFIG.url().isBlank()) {
      try (PsqlStorage psqlStorage = new PsqlStorage(STORAGE_CONFIG.url())) {
        psqlStorage.dropSchema();
      }
    }
  }
}
