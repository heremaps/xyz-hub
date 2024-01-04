package com.here.naksha.app.init.context;

public class TestContextEntrypoint {

  public static final TestContext TEST_CONTEXT = new ContainerTestContext();
//  public static final TestContext TEST_CONTEXT = new LocalTestContext();

  private TestContextEntrypoint(){}
}
