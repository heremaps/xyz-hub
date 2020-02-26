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
package com.here.xyz.hub.util.health.checks;

import static com.here.xyz.hub.util.health.Config.Setting.CHECK_DEFAULT_INTERVAL;
import static com.here.xyz.hub.util.health.Config.Setting.CHECK_DEFAULT_TIMEOUT;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.util.health.Config;
import com.here.xyz.hub.util.health.MainHealthCheck;
import com.here.xyz.hub.util.health.schema.Check;
import com.here.xyz.hub.util.health.schema.Response;
import com.here.xyz.hub.util.health.schema.Status;
import com.here.xyz.hub.util.health.schema.Status.Result;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A check which executes a specific action periodically and asynchronously.
 * As soon as a result exists the status will be available using {@link #getStatus()}
 * and also (if applicable) a response may be available using {@link #getResponse()}.
 * 
 * @see #setCheckInterval(int)
 * @see #setTimeout(int)
 * @see Config
 */
public abstract class ExecutableCheck extends Check implements Runnable {
	private static final Logger logger = LogManager.getLogger();

	protected static ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);
	
	protected int checkInterval = Config.getInt(CHECK_DEFAULT_INTERVAL);
	protected int timeout = Config.getInt(CHECK_DEFAULT_TIMEOUT);
	protected ScheduledFuture<?> executionHandle;

	protected boolean commenced = false;
	
	private String id = UUID.randomUUID().toString();
	
	public ExecutableCheck() {
		setStatus(new Status());
	}

	/**
	 * Begins executing this check periodically and asynchronously.
	 * Will only be called once by {@link MainHealthCheck#commence()},
	 * subsequent calls won't have an effect.
	 * 
	 * @return This check for chaining
	 */
	public ExecutableCheck commence() {
		if (!commenced) {
			commenced = true;
			executionHandle = executorService.scheduleWithFixedDelay(this, 0, checkInterval, TimeUnit.MILLISECONDS);
		}
		return this;
	}

	/**
	 * Stops the periodic execution of this check.
	 * Once it has been calls subsequent calls won't have an effect before {@link #commence()} has been called.
	 *
	 * @return This check for chaining
	 */
	public ExecutableCheck quit() {
		if (commenced) {
			executionHandle.cancel(false);
			commenced = false;
		}
		return this;
	}
	
	public void run() {
		try {
			final long t1 = Service.currentTimeMillis();
			try {
				executorService.submit(() -> {
					Status s = execute();
					final long t2 = Service.currentTimeMillis();
					s.setCheckDuration(t2 - t1);
					s.setTimestamp(t2);
					setStatus(s);
				}).get(timeout, TimeUnit.MILLISECONDS);
			}
			catch (TimeoutException e) {
				Status s = new Status();
				s.setResult(Result.TIMEOUT);
				final long t2 = Service.currentTimeMillis();
				s.setCheckDuration(t2 - t1);
				s.setTimestamp(t2);
				setStatus(s);
			}
		}
		catch (Throwable t) {
			logger.error("{}: Error when executing check", this.getClass().getSimpleName(), t );
		}
	}
	
	/**
	 * Executes the check.
	 * This method does whatever is needed to check if some specific dependency is 
	 * provided correctly.
	 * That could be a component of the service itself, but also remote components like
	 * databases or other services.
	 * 
	 * Besides its return value this method can set / update following details on this check
	 * upon completion:
	 * - {@link Check#setResponse(Response)}
	 * - {@link Check#setAdditionalProperty(String, Object)} (For arbitrary non-standard details)
	 * 
	 * The following details will be set automatically by the health-check system:
	 * - {@link Check#setStatus(Status)}
	 * - {@link Check#setTarget(Target)}
	 * - {@link Check#setRole(Role)}
	 * 
	 * @see Check#getRole()
	 * @return A {@link Status} object reflecting the minimal required result of a check.
	 */
	public abstract Status execute();
	
	protected Result getWorseResult(Result r1, Result r2) {
		if (r1.compareTo(r2) > 0) return r1;
		return r2;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object other) {
		if (!(other instanceof ExecutableCheck)) return super.equals(other);
		return super.equals(other) && ((ExecutableCheck) other).id.equals(id);
	}
	
	private static class TimeoutValueFilter {
		@Override
		public boolean equals(Object obj) {
			return obj == null || Config.getInt(CHECK_DEFAULT_TIMEOUT) == ((Integer) obj);
		}
	}

	//Necessary workaround as Include.NON_DEFAULT doesn't work for getters
	@JsonInclude(value = Include.CUSTOM, valueFilter = TimeoutValueFilter.class)
	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	private static class CheckIntervalValueFilter {
		@Override
		public boolean equals(Object obj) {
			return obj == null || Config.getInt(CHECK_DEFAULT_INTERVAL) == ((Integer) obj);
		}
	}

	//Necessary workaround as Include.NON_DEFAULT doesn't work for getters
	@JsonInclude(value = Include.CUSTOM, valueFilter = CheckIntervalValueFilter.class)
	public int getCheckInterval() {
		return checkInterval;
	}

	public void setCheckInterval(int checkInterval) {
		this.checkInterval = checkInterval;
	}
	
}
