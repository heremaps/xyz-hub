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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.util.IOUtils;
import com.here.xyz.hub.Service;

/**
 * The {@link S3WebMessageBroker} extends the {@link WebMessageBroker} abstract
 * implementation of how to send & receive {@link AdminMessage}s.
 * 
 * To use the {@link S3WebMessageBroker} you can set the environment variable
 * "ADMIN_MESSAGE_BROKER={@link S3WebMessageBroker}".
 * 
 * The {@link S3WebMessageBroker} must be configured. You can set the
 * environment variable "ADMIN_MESSAGE_BROKER_CONFIG".
 * 
 */

public class S3WebMessageBroker extends WebMessageBroker {

	private static volatile S3WebMessageBroker instance;
	private static volatile AmazonS3 S3_CLIENT;
	private static volatile AmazonS3URI S3_URI;
	private static volatile Boolean isInitialized;

	static {
		try {
			S3_URI = new AmazonS3URI((Service.configuration.ADMIN_MESSAGE_BROKER_CONFIG != null
					? Service.configuration.ADMIN_MESSAGE_BROKER_CONFIG.get("s3Uri")
					: "s3://xyz-hub-admin/service-instances.json"));
			S3_CLIENT = AmazonS3ClientBuilder.standard().withCredentials(new DefaultAWSCredentialsProviderChain())
					.build();
			setPeriodicUpdateConfig();
			isInitialized = true;
			logger.info("The S3WebMessageBroker was initialized.");
		} catch (Exception e) {
			S3_URI = null;
			S3_CLIENT = null;
			disablePeriodicUpdate();
			isInitialized = false;
			logger.warn("Initializing the S3WebMessageBroker failed with error: {} ", e.getMessage());
		}
		instance = new S3WebMessageBroker();
	}

	@Override
	protected Boolean isInitialized() {
		return isInitialized;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected ConcurrentHashMap<String, String> getTargetEndpoints() throws Exception {
		return mapper.get().readValue(
				new String(IOUtils
						.toByteArray(S3_CLIENT.getObject(S3_URI.getBucket(), S3_URI.getKey()).getObjectContent())),
				ConcurrentHashMap.class);
	}

	public static S3WebMessageBroker getInstance() {
		return instance;
	}

}
