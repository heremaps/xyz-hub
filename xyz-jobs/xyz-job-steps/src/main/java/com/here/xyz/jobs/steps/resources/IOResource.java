package com.here.xyz.jobs.steps.resources;

import io.vertx.core.Future;

public class IOResource extends ExecutionResource {

  private static final IOResource INSTANCE = new IOResource();

  private IOResource() {}

  public static IOResource getInstance() {return INSTANCE;}

  @Override
  public Future<Double> getUtilizedUnits() {
    return Future.succeededFuture((double) 0);
  }

  @Override
  protected double getMaxUnits() {
    return Long.MAX_VALUE;
  }

  @Override
  protected double getMaxVirtualUnits() {
    return Long.MAX_VALUE;
  }

  @Override
  protected String getId() {
    return "io-execution-resource";
  }
}
