/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

import static com.here.xyz.hub.util.health.schema.Status.Result.CRITICAL;
import static com.here.xyz.hub.util.health.schema.Status.Result.ERROR;
import static com.here.xyz.hub.util.health.schema.Status.Result.OK;
import static com.here.xyz.hub.util.health.schema.Status.Result.UNKNOWN;

import com.here.xyz.hub.cache.RedisCacheClient;
import com.here.xyz.hub.util.health.schema.Response;
import com.here.xyz.hub.util.health.schema.Status;
import java.util.Arrays;


public class RedisHealthCheck extends ExecutableCheck {

	private static final String HC_CACHE_KEY = "__SAMPLE_HEALTH_CHECK_KEY";
	private static final byte[] HC_CACHE_VALUE = "someValue".getBytes();
	private final String host;
	private final int port;
	private RedisCacheClient client;
	private volatile byte[] lastReceivedValue;

	public RedisHealthCheck(String host, int port) {
		this.host = host;
		this.port = port;
		setName("Redis");
		setRole(Role.CACHE);
		setTarget(Target.REMOTE);
	}

	@Override
	public Status execute() {
		Status s = new Status();
		Response r = new Response();
		r.setNode(host + ":" + port);
		if (host == null) {
			setResponse(r.withMessage("No Redis host given."));
			return s.withResult(UNKNOWN);
		}

		try {
			if (client == null) {
				client = new RedisCacheClient();
			}
		}
		catch (Throwable t) {
      setResponse(r.withMessage("Error when trying to create Redis client: " + t.getMessage()));
			return s.withResult(ERROR);
		}

		//Try setting a value
		try {
			client.set(HC_CACHE_KEY, HC_CACHE_VALUE, 10);
		}
		catch (Throwable t) {
      setResponse(r.withMessage("Error when trying to set the sample health check record."));
			resetClient();
			return s.withResult(ERROR);
		}

		//Try getting the value back
		lastReceivedValue = null;
		try {
			client.get(HC_CACHE_KEY, result -> {
				lastReceivedValue = result;
				synchronized (this) {
					this.notify();
				}
			});
		}
		catch (Throwable t) {
      setResponse(r.withMessage("Error when trying to get the sample health check record back."));
			resetClient();
			return s.withResult(ERROR);
		}
		try {
			synchronized (this) {
				this.wait(timeout);
			}
		} catch (InterruptedException e) {
			resetClient();
			return s.withResult(UNKNOWN);
		}
		if (Arrays.equals(HC_CACHE_VALUE, lastReceivedValue)) {
			setResponse(null);
			return s.withResult(OK);
		}
		setResponse(r.withMessage("Wasn't able to retrieve the sample health check record back correctly."));
		resetClient();
		return s.withResult(CRITICAL);
	}

	/**
	 * Resets the client when it might be that the connection needs to be re-established
	 */
	private void resetClient() {
		client.shutdown();
		client = null;
	}

}
