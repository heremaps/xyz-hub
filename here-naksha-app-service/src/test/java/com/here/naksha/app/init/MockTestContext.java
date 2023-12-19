package com.here.naksha.app.init;

import static com.here.naksha.app.service.NakshaApp.newInstance;

import com.here.naksha.app.service.NakshaApp;

public class MockTestContext extends TestContext {

  private static final String CONFIG_ID = "mock-config";

  public MockTestContext() {
    super(() -> newInstance(CONFIG_ID));
  }
}
