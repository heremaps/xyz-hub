package com.here.naksha.app.init.context;

import static com.here.naksha.app.service.NakshaApp.newInstance;

class MockTestContext extends TestContext {

  private static final String CONFIG_ID = "mock-config";

  MockTestContext() {
    super(() -> newInstance(CONFIG_ID));
  }
}
