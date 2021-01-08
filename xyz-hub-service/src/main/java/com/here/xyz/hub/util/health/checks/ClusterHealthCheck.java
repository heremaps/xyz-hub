package com.here.xyz.hub.util.health.checks;

import static com.here.xyz.hub.util.health.schema.Status.Result.ERROR;
import static com.here.xyz.hub.util.health.schema.Status.Result.OK;

import com.here.xyz.hub.rest.admin.Node;
import com.here.xyz.hub.util.health.schema.Response;
import com.here.xyz.hub.util.health.schema.Status;

public class ClusterHealthCheck extends ExecutableCheck {

  public ClusterHealthCheck() {
    setName("Cluster");
    setTarget(Target.REMOTE);
  }

  @Override
  public Status execute() {
    Status s = new Status().withResult(OK);
    Response r = new Response();
    try {
      r.setAdditionalProperty("nodes", Node.getClusterNodes());
      s.setResult(OK);
    }
    catch (Exception e) {
      r.setMessage("Error: " + e.getMessage());
      s.setResult(ERROR);
    }
    setResponse(r);
    return s;
  }
}
