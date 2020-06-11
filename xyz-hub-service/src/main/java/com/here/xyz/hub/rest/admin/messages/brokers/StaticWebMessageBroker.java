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

package com.here.xyz.hub.rest.admin.messages.brokers;

import java.util.concurrent.ConcurrentHashMap;

import com.here.xyz.hub.Service;

/**
 * The {@link StaticWebMessageBroker} extends the {@link WebMessageBroker}
 * abstract implementation of how to send & receive {@link AdminMessage}s.
 * 
 * To use the {@link StaticWebMessageBroker} you can set the environment
 * variable "ADMIN_MESSAGE_BROKER={@link StaticWebMessageBroker}".
 * 
 * The {@link StaticWebMessageBroker} must be configured. You can set the
 * environment variable "STATIC_WEB_MESSAGE_BROKER_CONFIG" to a json string,
 * e.g. { "instance": "port", "instance": "port", ... }.
 * 
 */

public class StaticWebMessageBroker extends WebMessageBroker {

	private static volatile StaticWebMessageBroker instance;
	private static volatile ConcurrentHashMap<String, String> STATIC_WEB_MESSAGE_BROKER_CONFIG;
	private static volatile Boolean isInitialized;

	static {
		STATIC_WEB_MESSAGE_BROKER_CONFIG = (Service.configuration.STATIC_WEB_MESSAGE_BROKER_CONFIG != null
				? Service.configuration.STATIC_WEB_MESSAGE_BROKER_CONFIG
				: new ConcurrentHashMap<String, String>());
		disablePeriodicUpdate();
		isInitialized = true;
		logger.info("The StaticWebMessageBroker was initialized.");
		instance = new StaticWebMessageBroker();
	}

	@Override
	protected Boolean isInitialized() {
		return isInitialized;
	}

	@Override
	protected ConcurrentHashMap<String, String> getTargetEndpoints() throws Exception {
		return STATIC_WEB_MESSAGE_BROKER_CONFIG;
	}

	public static StaticWebMessageBroker getInstance() {
		return instance;
	}

}
