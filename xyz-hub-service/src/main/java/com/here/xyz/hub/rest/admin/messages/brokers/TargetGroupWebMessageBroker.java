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
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingAsync;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingAsyncClientBuilder;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTargetHealthRequest;
import com.here.xyz.hub.Service;

/**
 * The {@link TargetGroupWebMessageBroker} extends the {@link WebMessageBroker}
 * abstract implementation of how to send & receive {@link AdminMessage}s.
 * 
 * To use the {@link TargetGroupWebMessageBroker} you can set the environment
 * variable "ADMIN_MESSAGE_BROKER={@link TargetGroupWebMessageBroker}".
 * 
 * The {@link TargetGroupWebMessageBroker} must be configured. You can set the
 * environment variable "TARGET_GROUP_WEB_MESSAGE_BROKER_ELB_TARGETGROUP_ARN".
 * 
 */

public class TargetGroupWebMessageBroker extends WebMessageBroker {

	private static volatile TargetGroupWebMessageBroker instance;
	private static volatile AmazonElasticLoadBalancingAsync ELB_CLIENT;
	private static volatile String TARGET_GROUP_WEB_MESSAGE_BROKER_ELB_TARGETGROUP_ARN;
	private static volatile Boolean isInitialized;

	static {
		TARGET_GROUP_WEB_MESSAGE_BROKER_ELB_TARGETGROUP_ARN = (Service.configuration.TARGET_GROUP_WEB_MESSAGE_BROKER_ELB_TARGETGROUP_ARN != null
				? Service.configuration.TARGET_GROUP_WEB_MESSAGE_BROKER_ELB_TARGETGROUP_ARN
				: null);
		try {
			ELB_CLIENT = AmazonElasticLoadBalancingAsyncClientBuilder.standard()
					.withCredentials(new DefaultAWSCredentialsProviderChain()).build();
			setPeriodicUpdateConfig();
			isInitialized = true;
			logger.info("The TargetGroupWebMessageBroker was initialized.");
		} catch (Exception e) {
			ELB_CLIENT = null;
			disablePeriodicUpdate();
			isInitialized = false;
			logger.warn("Initializing the TargetGroupWebMessageBroker failed with error: {} ", e.getMessage());
		}
		instance = new TargetGroupWebMessageBroker();
	}

	@Override
	protected Boolean isInitialized() {
		return isInitialized;
	}

	@Override
	protected ConcurrentHashMap<String, String> getTargetEndpoints() throws Exception {
		// TODO: minor: support multiple target group arn once required
		ConcurrentHashMap<String, String> targetEndpoints = new ConcurrentHashMap<String, String>();
		DescribeTargetHealthRequest request = new DescribeTargetHealthRequest();
		request.setTargetGroupArn(TARGET_GROUP_WEB_MESSAGE_BROKER_ELB_TARGETGROUP_ARN);
		// TODO: minor: respect possible pagination
		ELB_CLIENT.describeTargetHealth(request).getTargetHealthDescriptions().forEach(targetInstance -> targetEndpoints
				.put(targetInstance.getTarget().getId(), Integer.toString(targetInstance.getTarget().getPort())));
		return targetEndpoints;
	}

	public static TargetGroupWebMessageBroker getInstance() {
		return instance;
	}

}
