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

package com.here.xyz.hub.rest.admin;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.auth.Authorization.AuthorizationType;
import com.here.xyz.hub.rest.AdminApi;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

/**
 * The abstract {@link WebMessageBroker} implements the {@link MessageBroker}
 * interface.
 *
 * For extending this abstract you must implement
 * {@link WebMessageBroker#isInitialized()},
 * {@link WebMessageBroker#getTargetEndpoints()} and a static
 * {@link #getInstance()} method.
 *
 * The {@link WebMessageBroker} can be configured. You can set the environment
 * variable "WEB_MESSAGE_BROKER_PERIODIC_UPDATE" to a boolean and you can set
 * the environment variable "WEB_MESSAGE_BROKER_PERIODIC_UPDATE_DELAY" to an
 * integer.
 */

abstract class WebMessageBroker implements MessageBroker {
	protected static final Logger logger = LogManager.getLogger();

	protected static final ThreadLocal<ObjectMapper> mapper = ThreadLocal.withInitial(ObjectMapper::new);
	private static final long MAX_MESSAGE_SIZE = 256 * 1024;

	private static volatile WebClient HTTP_CLIENT;
	private static volatile ConcurrentHashMap<String, String> TARGET_ENDPOINTS;
	protected static volatile Boolean WEB_MESSAGE_BROKER_PERIODIC_UPDATE;
	protected static volatile Integer WEB_MESSAGE_BROKER_PERIODIC_UPDATE_DELAY;

	protected WebMessageBroker() {
		HTTP_CLIENT = WebClient.create(Service.vertx);
		updateTargetEndpoints();
		if (WEB_MESSAGE_BROKER_PERIODIC_UPDATE) {
			Service.vertx.setPeriodic(WEB_MESSAGE_BROKER_PERIODIC_UPDATE_DELAY, handler -> updateTargetEndpoints());
		}
	}

	abstract protected Boolean isInitialized();

	abstract protected ConcurrentHashMap<String, String> getTargetEndpoints() throws Exception;

	private void updateTargetEndpoints() {
		if (isInitialized()) {
			try {
				TARGET_ENDPOINTS = removeOwnInstance(getTargetEndpoints());
			} catch (Exception e) {
				logger.warn("Failed to update target endpoints with error {} ", e.getMessage());
			}
		} else {
			logger.warn("Failed to update target endpoints. The broker is not initialized!");
		}
		logConfig();
	}

	private ConcurrentHashMap<String, String> removeOwnInstance(ConcurrentHashMap<String, String> targetEndpoints) {
		InetAddress ownInetAddress;
		String targetInetAddress;
		try {
			ownInetAddress = InetAddress.getLocalHost();
			for (String key : targetEndpoints.keySet()) {
				try {
					targetInetAddress = InetAddress.getByName(key).getHostAddress();
				} catch (Exception targetInetAddressException) {
					logger.debug("Failed to resolve target host address.");
					targetInetAddress = "";
				}
				if (key.equals(Service.configuration.HOST_NAME) || key.equals(ownInetAddress.getHostAddress())
						|| key.equals(ownInetAddress.getHostName())
						|| ownInetAddress.getHostAddress().equals(targetInetAddress)) {
					logger.debug("Removing own instance ({}:{}) from target endpoints.", key, targetEndpoints.get(key));
					targetEndpoints.remove(key);
				}
			}
		} catch (Exception ownInetAddressException) {
			logger.debug("Failed to resolve local host address.");
		}
		return targetEndpoints;
	}

	@Override
	public void sendMessage(AdminMessage message) {
		logger.debug("Send AdminMessage.@Class: {} , Source.Ip: {}", message.getClass().getSimpleName(),
				message.source.ip);
		if (!Node.OWN_INSTANCE.equals(message.destination)) {
			String jsonMessage = null;
			try {
				jsonMessage = mapper.get().writeValueAsString(message);
				sendMessage(jsonMessage);
			} catch (JsonProcessingException e) {
				logger.warn("Error while serializing AdminMessage of type {} prior to send it.",
						message.getClass().getSimpleName());
			} catch (Exception e) {
				logger.warn("Error while sending AdminMessage: {}", e.getMessage());
			}
			logger.debug("AdminMessage was: {}", jsonMessage);
		}
		// Receive it (also) locally (if applicable)
		/*
		 * NOTE: Local messages will always be received directly and only once. This is
		 * also true for a broadcast message with the #broadcastIncludeLocalNode flag
		 * being active.
		 */
		receiveMessage(message);
	}

	private void sendMessage(String message) {
		if (HTTP_CLIENT == null) {
			logger.warn("The AdminMessage cannot be processed. The HTTP_CLIENT is not ready. AdminMessage was: {}",
					message);
			return;
		}
		if (message.length() > MAX_MESSAGE_SIZE) {
			throw new RuntimeException(
					"The AdminMessage cannot be processed. The AdminMessage is larger than the MAX_MESSAGE_SIZE.");
		}
		if (TARGET_ENDPOINTS != null && TARGET_ENDPOINTS.size() > 0) {
			logConfig();
			@SuppressWarnings("rawtypes")
			List<Future> targetEndpointRequests = new ArrayList<>();
			for (String key : TARGET_ENDPOINTS.keySet()) {
				logger.debug("Preparing request for target: {}:{}", key, TARGET_ENDPOINTS.get(key));
				targetEndpointRequests.add(notifyEndpoint(key, TARGET_ENDPOINTS.get(key), message));
			}
			CompositeFuture.join(targetEndpointRequests).setHandler(handler -> {
				if (!handler.succeeded()) {
					logger.warn(
							"Send AdminMessage to all target endpoints ends with failure. Some requests did not complete.");
				}
			});
			logger.debug("Send AdminMessage to all target endpoints running in background.");
		} else {
			logger.warn("Send AdminMessage cannot run. The WebMessageBroker has no target endpoints.");
		}
	}

	@Override
	public void receiveRawMessage(byte[] rawJsonMessage) {
		if (rawJsonMessage == null) {
			logger.warn("No bytes given for receiving the AdminMessage.", new NullPointerException());
			return;
		}
		try {
			receiveMessage(mapper.get().readTree(new String(rawJsonMessage)).asText());
		} catch (Exception e) {
			logger.warn("Error while de-serializing the received raw AdminMessage {} : {}", new String(rawJsonMessage),
					e.getMessage());
		}
	}

	private void receiveMessage(String jsonMessage) {
		AdminMessage message;
		try {
			message = mapper.get().readValue(jsonMessage, AdminMessage.class);
			receiveMessage(message);
		} catch (IOException e) {
			logger.warn("Error while de-serializing the received AdminMessage {} : {}", jsonMessage, e.getMessage());
		} catch (Exception e) {
			logger.warn("Error while receiving the received AdminMessage {} : {}", jsonMessage, e.getMessage());
		}
	}

	private void receiveMessage(AdminMessage message) {
		if (message.source == null) {
			throw new NullPointerException("The source node of the AdminMessage must be defined.");
		}
		if (message.destination == null
				&& (!Node.OWN_INSTANCE.equals(message.source) || message.broadcastIncludeLocalNode)
				|| Node.OWN_INSTANCE.equals(message.destination)) {
			try {
				logger.debug("Handle AdminMessage.@Class: {} , Source.Ip: {}", message.getClass().getSimpleName(),
						message.source.ip);
				message.handle();
			} catch (RuntimeException e) {
				logger.warn("Error while trying to handle the received AdminMessage {} : {}", message, e.getMessage());
			}
		} else {
			logger.debug("Drop AdminMessage.@Class: {} , Source.Ip: {}", message.getClass().getSimpleName(),
					message.source.ip);
		}
	}

	private Future<String> notifyEndpoint(String endpointName, String endpointPort, String message) {
		Future<String> future = Future.future();
		if (Service.configuration.XYZ_HUB_AUTH == AuthorizationType.DUMMY) {
			HTTP_CLIENT.post(Integer.parseInt(endpointPort), endpointName, AdminApi.ADMIN_MESSAGES_ENDPOINT).sendJson(
					message, handler -> handleNotificationResult(endpointName, endpointPort, future, handler));
		} else {
			HTTP_CLIENT.post(Integer.parseInt(endpointPort), endpointName, AdminApi.ADMIN_MESSAGES_ENDPOINT)
					.putHeader("Authorization", "bearer " + Service.configuration.ADMIN_MESSAGE_JWT).sendJson(message,
							handler -> handleNotificationResult(endpointName, endpointPort, future, handler));
		}
		return future;
	}

	private void handleNotificationResult(String endpointName, String endpointPort, Future<String> future,
			AsyncResult<HttpResponse<Buffer>> handler) {
		if (handler.succeeded()) {
			if (handler.result().statusCode() != 200 && handler.result().statusCode() != 204) {
				logger.warn("The WebMessageBroker HTTP_CLIENT failed to post data to endpoint {}:{}. The error is: {}",
						endpointName, endpointPort,
						"Unexpected status code " + Integer.toString(handler.result().statusCode()));
				future.fail("Unexpected status code " + Integer.toString(handler.result().statusCode()));
			} else {
				future.complete(Integer.toString(handler.result().statusCode()));
			}
		} else {
			logger.warn("The WebMessageBroker HTTP_CLIENT failed to post data to endpoint {}:{}. The error is: {}",
					endpointName, endpointPort, handler.cause().getMessage());
			future.fail(handler.cause().getMessage());
		}
	}

	protected static void setPeriodicUpdateConfig() {
		WEB_MESSAGE_BROKER_PERIODIC_UPDATE = (Service.configuration.WEB_MESSAGE_BROKER_PERIODIC_UPDATE != null
				? Service.configuration.WEB_MESSAGE_BROKER_PERIODIC_UPDATE
				: false);
		WEB_MESSAGE_BROKER_PERIODIC_UPDATE_DELAY = (Service.configuration.WEB_MESSAGE_BROKER_PERIODIC_UPDATE_DELAY != null
				? Service.configuration.WEB_MESSAGE_BROKER_PERIODIC_UPDATE_DELAY
				: 30000);
		if (WEB_MESSAGE_BROKER_PERIODIC_UPDATE && WEB_MESSAGE_BROKER_PERIODIC_UPDATE_DELAY < 30000) {
			WEB_MESSAGE_BROKER_PERIODIC_UPDATE_DELAY = 30000;
		}
		if (WEB_MESSAGE_BROKER_PERIODIC_UPDATE && WEB_MESSAGE_BROKER_PERIODIC_UPDATE_DELAY <= 0) {
			WEB_MESSAGE_BROKER_PERIODIC_UPDATE = false;
		}
	}

	protected static void disablePeriodicUpdate() {
		WEB_MESSAGE_BROKER_PERIODIC_UPDATE = false;
		WEB_MESSAGE_BROKER_PERIODIC_UPDATE_DELAY = 0;
	}

	protected void logConfig() {
		logger.debug("TARGET_ENDPOINTS: {}, PeriodicUpdate: {}, PeriodicUpdateDelay: {}", TARGET_ENDPOINTS,
				WEB_MESSAGE_BROKER_PERIODIC_UPDATE, WEB_MESSAGE_BROKER_PERIODIC_UPDATE_DELAY);
	}

}