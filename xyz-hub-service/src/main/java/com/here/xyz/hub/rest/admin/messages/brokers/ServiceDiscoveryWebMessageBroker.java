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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.servicediscovery.AWSServiceDiscoveryAsync;
import com.amazonaws.services.servicediscovery.AWSServiceDiscoveryAsyncClientBuilder;
import com.amazonaws.services.servicediscovery.model.ListInstancesRequest;
import com.here.xyz.hub.Service;

/**
 * The {@link ServiceDiscoveryWebMessageBroker} extends the
 * {@link WebMessageBroker} abstract implementation of how to send & receive
 * {@link AdminMessage}s.
 * 
 * To use the {@link ServiceDiscoveryWebMessageBroker} you can set the
 * environment variable
 * "ADMIN_MESSAGE_BROKER={@link ServiceDiscoveryWebMessageBroker}".
 * 
 * The {@link ServiceDiscoveryWebMessageBroker} must be configured. You can set
 * the environment variable "ADMIN_MESSAGE_BROKER_CONFIG".
 * 
 */

public class ServiceDiscoveryWebMessageBroker extends WebMessageBroker {

	private static volatile ServiceDiscoveryWebMessageBroker instance;
	private static volatile AWSServiceDiscoveryAsync SD_CLIENT;
	private static volatile String SD_ID;
	private static volatile Boolean isInitialized;

	static {
		try {
			SD_ID = (Service.configuration.ADMIN_MESSAGE_BROKER_CONFIG.get("serviceDiscoveryId") != null
					? Service.configuration.ADMIN_MESSAGE_BROKER_CONFIG.get("serviceDiscoveryId")
					: "xyz-hub");
			SD_CLIENT = AWSServiceDiscoveryAsyncClientBuilder.standard()
					.withCredentials(new DefaultAWSCredentialsProviderChain()).build();
			setPeriodicUpdateConfig();
			isInitialized = true;
			logger.info("The ServiceDiscoveryWebMessageBroker was initialized.");
		} catch (Exception e) {
			logger.warn("Initializing the ServiceDiscoveryWebMessageBroker failed with error: {} ", e.getMessage());
			SD_CLIENT = null;
			disablePeriodicUpdate();
			isInitialized = false;
		}
		instance = new ServiceDiscoveryWebMessageBroker();
	}

	@Override
	protected Boolean isInitialized() {
		return isInitialized;
	}

	@Override
	protected ConcurrentHashMap<String, String> getTargetEndpoints() throws Exception {
		ConcurrentHashMap<String, String> targetEndpoints = new ConcurrentHashMap<String, String>();
		ListInstancesRequest request = new ListInstancesRequest();
		request.setServiceId(SD_ID);
		// TODO: minor: respect possible pagination
		SD_CLIENT.listInstances(request).getInstances()
				.forEach(targetInstance -> targetEndpoints.put(targetInstance.getAttributes().get("AWS_INSTANCE_IPV4"),
						targetInstance.getAttributes().get("AWS_INSTANCE_PORT")));
		return targetEndpoints;
	}

	public static ServiceDiscoveryWebMessageBroker getInstance() {
		return instance;
	}

}
