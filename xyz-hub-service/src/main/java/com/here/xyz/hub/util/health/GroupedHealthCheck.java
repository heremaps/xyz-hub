package com.here.xyz.hub.util.health;

import static com.here.xyz.hub.util.health.schema.Status.Result.CRITICAL;
import static com.here.xyz.hub.util.health.schema.Status.Result.OK;
import static com.here.xyz.hub.util.health.schema.Status.Result.UNKNOWN;
import static com.here.xyz.hub.util.health.schema.Status.Result.WARNING;

import com.here.xyz.hub.util.health.checks.ExecutableCheck;
import com.here.xyz.hub.util.health.schema.Response;
import com.here.xyz.hub.util.health.schema.Status;
import com.here.xyz.hub.util.health.schema.Status.Result;
import java.util.LinkedHashSet;
import java.util.Set;

public abstract class GroupedHealthCheck extends ExecutableCheck {

  protected Set<ExecutableCheck> checks = new LinkedHashSet<>();

  /**
   * Adds another check as dependency to this GroupedHealthCheck. If this GroupedHealthCheck already has commenced the added check will
   * automatically be commenced as well. Following responses of this GroupedHealthCheck will then include also the result of the new check.
   *
   * @param c The check to be added and checked from then on
   * @return This check for chaining
   */
  public GroupedHealthCheck add(ExecutableCheck c) {
    checks.add(c);
    if (commenced) {
      c.commence();
    }
    return this;
  }

  public GroupedHealthCheck remove(ExecutableCheck c) {
    if (checks.contains(c)) {
      c.quit();
      checks.remove(c);
    }
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Status execute() {
    return collectStatus();
  }

  private Status collectStatus() {
    Status status = new Status();
    Response response = new Response();

    Result r = OK;
    for (ExecutableCheck check : checks) {
      Status checkStatus = check.getStatus();
      if (check.getEssential()) {
				if (checkStatus.getResult().compareTo(UNKNOWN) >= 0) {
					r = CRITICAL;
				} else {
					r = getWorseResult(r, checkStatus.getResult());
				}
      } else if (checkStatus.getResult() != OK) {
        r = getWorseResult(r, WARNING);
      }
    }

    //Report the new status
    status.setResult(r);

    //Collect all checks & their responses and report the new response
    response.getChecks().addAll(checks);
    setResponse(response);

    return status;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ExecutableCheck commence() {
    if (!commenced) {
      checks.forEach(ExecutableCheck::commence);
    }
    return super.commence();
  }
}
