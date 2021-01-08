package com.here.xyz.hub.util.health.checks;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.util.health.schema.Response;
import com.here.xyz.hub.util.health.schema.Status;
import com.here.xyz.hub.util.health.schema.Status.Result;

public class MemoryHealthCheck extends ExecutableCheck {

  public MemoryHealthCheck() {
    setName("Memory");
    setTarget(Target.LOCAL);
  }

  @Override
  public Status execute() {
    Status s = new Status().withResult(Result.OK);
    Response r = new Response();
    attachMemoryInfo(r);
    setResponse(r);
    return s;
  }

  private void attachMemoryInfo(Response r) {
    try {
      r.setAdditionalProperty("usedMemoryBytes", Service.getUsedMemoryBytes());
      r.setAdditionalProperty("usedMemoryPercent", Service.getUsedMemoryPercent());
    }
    catch (Exception e) {
      r.setAdditionalProperty("usedMemoryBytes", "N/A");
      r.setAdditionalProperty("usedMemoryPercent", "N/A");
    }
  }
}
