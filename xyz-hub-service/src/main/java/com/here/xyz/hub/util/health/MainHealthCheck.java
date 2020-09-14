/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */
package com.here.xyz.hub.util.health;

import static com.here.xyz.hub.util.health.Config.Setting.BOOT_GRACE_TIME;
import static com.here.xyz.hub.util.health.schema.Status.Result.CRITICAL;
import static com.here.xyz.hub.util.health.schema.Status.Result.OK;
import static com.here.xyz.hub.util.health.schema.Status.Result.UNKNOWN;
import static com.here.xyz.hub.util.health.schema.Status.Result.WARNING;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.util.health.checks.ExecutableCheck;
import com.here.xyz.hub.util.health.checks.FunctionCheck;
import com.here.xyz.hub.util.health.checks.ServiceHealthCheck;
import com.here.xyz.hub.util.health.schema.Reporter;
import com.here.xyz.hub.util.health.schema.Response;
import com.here.xyz.hub.util.health.schema.Status;
import com.here.xyz.hub.util.health.schema.Status.Result;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.apache.logging.log4j.core.util.CachedClock;

public class MainHealthCheck extends GroupedHealthCheck {

  private Reporter reporter;
  private long graceUntil;
  private boolean autoCommence;

  public MainHealthCheck(boolean autoCommence) {
    this.autoCommence = autoCommence;
    initialize();
  }

  public final void initialize() {
    if (isGracePeriodActivated()) {
      if (autoCommence) {
        System.err.println("WARNING: \"autoCommencing\" is activated in combination with "
            + "having set " + Config.getEnvName(BOOT_GRACE_TIME) + " for the MainHealthCheck. "
            + "The expected initialization time couldn't be evaluated. To prevent "
            + Config.getEnvName(BOOT_GRACE_TIME) + " from being potentially too small, please "
            + "consider deactivating \"autoCommencing\" by using the MainHealthCheck(false) "
            + "constructor and calling commence() after having added all checks!");
      }
      long remainingGraceTime = Config.getInt(BOOT_GRACE_TIME) - ManagementFactory.getRuntimeMXBean().getUptime();
      if (remainingGraceTime < 0) {
        System.err.println("WARNING: Your BOOT_GRACE_TIME ("
            + Config.getInt(BOOT_GRACE_TIME) + "ms) was already exceeded by "
            + "the start time of the JVM and other operations done before initializing "
            + "the CI/CD health check system. Please consider increasing the boot "
            + "grace time (by at least " + -remainingGraceTime + "ms) by setting the "
            + "environment variable " + Config.getEnvName(BOOT_GRACE_TIME) + "!");
      }
      int maxTimeout = checks.isEmpty() ? 0 : checks.stream().mapToInt(ExecutableCheck::getTimeout).max().getAsInt();
      if (remainingGraceTime < maxTimeout) {
        System.err.println("WARNING: Your BOOT_GRACE_TIME (" + Config.getInt(BOOT_GRACE_TIME) + "ms) "
            + "could be exceeded by one of the health check timeouts. Note that also the startup of the JVM "
            + "and other operations done before initializing the CI/CD health check system already took "
            + "some time. Please consider increasing the boot grace time (by at least "
            + (maxTimeout - remainingGraceTime) + "ms) by setting the environment variable "
            + Config.getEnvName(BOOT_GRACE_TIME) + "! Otherwise this could lead to blocking health check "
            + "requests which might disturb dependent systems.");
      }
      graceUntil = Service.currentTimeMillis() + remainingGraceTime;
    }

    Response r = new Response();
    r.setStatus(getStatus());
    setResponse(r);
		if (autoCommence) {
			commence();
		}
  }

  public boolean isGracePeriodActivated() {
    return Config.getInt(BOOT_GRACE_TIME) > 0;
  }

  private boolean isGraceTimeElapsed() {
    return Service.currentTimeMillis() > graceUntil;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final Status execute() {
    if (isGracePeriodActivated() && !isGraceTimeElapsed()) {
      Status status = new Status();
      Response response = new Response();
      status.setResult(OK);
      response.setStatus(status);
      setResponse(response);
      return status;
    }

    return super.execute();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final Response getResponse() {
    /*
     * Directly do the execution of this MainHealthCheck here on-demand
     * as it's only about collecting the sub-results.
     */
    Status s = execute()
        .withTimestamp(CachedClock.instance().currentTimeMillis());
    setStatus(s);
    Response r = super.getResponse();
    r.setStatus(s);
    r.setReporter(reporter);
    return r;
  }

  public void shutdown() {
    ExecutableCheck.executorService.shutdownNow();
  }

  public void setReporter(Reporter r) {
    reporter = r;
  }

  public MainHealthCheck withReporter(Reporter r) {
    setReporter(r);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final MainHealthCheck commence() {
    if (!commenced) {
      commenced = true;
      checks.forEach(ExecutableCheck::commence);
    }
    return this;
  }

  //----- Some convenience methods -----

  @Override
  public MainHealthCheck add(ExecutableCheck c) {
    return (MainHealthCheck) super.add(c);
  }

  @Override
  public MainHealthCheck remove(ExecutableCheck c) {
    return (MainHealthCheck) super.remove(c);
  }

  public MainHealthCheck add(BooleanSupplier bs) {
    return (MainHealthCheck) add(new FunctionCheck(bs));
  }

  public MainHealthCheck add(BooleanSupplier bs, String name) {
    return (MainHealthCheck) add(new FunctionCheck(bs, name));
  }

  public MainHealthCheck add(String otherServiceEndpoint) throws MalformedURLException {
    return (MainHealthCheck) add(new ServiceHealthCheck(otherServiceEndpoint, null));
  }

  public MainHealthCheck add(String otherServiceEndpoint, String name) throws MalformedURLException {
    return (MainHealthCheck) add(new ServiceHealthCheck(otherServiceEndpoint, name));
  }
}
