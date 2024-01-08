package com.here.naksha.app.init.context;

import static org.apache.commons.lang3.StringUtils.isBlank;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestContextEntrypoint {

  private static final Logger log = LoggerFactory.getLogger(TestContextEntrypoint.class);

  private static final String TEST_CONTEXT_ENV = "NAKSHA_LOCAL_TEST_CONTEXT";
  private static final String MOCK_CONTEXT_ENV_VAL = "MOCK";
  private static final String LOCAL_STANDALONE_CONTEXT_ENV_VAL = "LOCAL_STANDALONE";
  private static final String TEST_CONTAINERS_CONTEXT_ENV_VAL = "TEST_CONTAINERS";

  private TestContextEntrypoint() {
  }

  public static TestContext loadTestContext() {
    log.info("Loading test context, checking {} environment variable", TEST_CONTEXT_ENV);
    String contextFromEnv = System.getenv(TEST_CONTEXT_ENV);
    if (contextFromEnv == null || isBlank(contextFromEnv)) {
      log.info("Undefined environment variable {}, using local standalone context", TEST_CONTEXT_ENV);
      return new LocalTestContext();
    }
    switch (contextFromEnv) {
      case MOCK_CONTEXT_ENV_VAL -> {
        return new MockTestContext();
      }
      case TEST_CONTAINERS_CONTEXT_ENV_VAL -> {
        return new ContainerTestContext();
      }
      case LOCAL_STANDALONE_CONTEXT_ENV_VAL -> {
        return new LocalTestContext();
      }
      default -> {
        log.info("Unknown value ({}) specified in environment variable {}, using local standalone context", contextFromEnv,
            TEST_CONTEXT_ENV);
        return new LocalTestContext();
      }
    }
  }
}
