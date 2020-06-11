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
 * environment variables "S3_WEB_MESSAGE_BROKER_BUCKET" and
 * "S3_WEB_MESSAGE_BROKER_OBJECT".
 * 
 */

public class S3WebMessageBroker extends WebMessageBroker {

	private static volatile S3WebMessageBroker instance;
	private static volatile AmazonS3 S3_CLIENT;
	private static volatile String S3_WEB_MESSAGE_BROKER_BUCKET;
	private static volatile String S3_WEB_MESSAGE_BROKER_OBJECT;
	private static volatile Boolean isInitialized;

	static {
		S3_WEB_MESSAGE_BROKER_BUCKET = (Service.configuration.S3_WEB_MESSAGE_BROKER_BUCKET != null
				? Service.configuration.S3_WEB_MESSAGE_BROKER_BUCKET
				: "xyz-hub-admin-messaging");
		S3_WEB_MESSAGE_BROKER_OBJECT = (Service.configuration.S3_WEB_MESSAGE_BROKER_OBJECT != null
				? Service.configuration.S3_WEB_MESSAGE_BROKER_OBJECT
				: "instances.json");
		try {
			S3_CLIENT = AmazonS3ClientBuilder.standard().withCredentials(new DefaultAWSCredentialsProviderChain())
					.build();
			setPeriodicUpdateConfig();
			isInitialized = true;
			logger.info("The S3WebMessageBroker was initialized.");
		} catch (Exception e) {
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
		return mapper.get()
				.readValue(new String(IOUtils.toByteArray(S3_CLIENT
						.getObject(S3_WEB_MESSAGE_BROKER_BUCKET, S3_WEB_MESSAGE_BROKER_OBJECT).getObjectContent())),
						ConcurrentHashMap.class);
	}

	public static S3WebMessageBroker getInstance() {
		return instance;
	}

}
