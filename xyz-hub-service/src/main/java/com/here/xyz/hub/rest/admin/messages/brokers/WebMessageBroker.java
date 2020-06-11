package com.here.xyz.hub.rest.admin.messages.brokers;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.auth.Authorization.AuthorizationType;
import com.here.xyz.hub.rest.AdminApi;
import com.here.xyz.hub.rest.admin.MessageBroker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

abstract class WebMessageBroker implements MessageBroker {

	protected static final Logger logger = LogManager.getLogger();

	private static volatile WebClient HTTP_CLIENT;
	private static volatile ConcurrentHashMap<String, String> TARGET_ENDPOINTS;
	protected static volatile Boolean WEB_MESSAGE_BROKER_PERIODIC_UPDATE;
	protected static volatile Integer WEB_MESSAGE_BROKER_PERIODIC_UPDATE_DELAY;
	private static final long MAX_MESSAGE_SIZE = 256 * 1024;

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
	public void receiveRawMessage(byte[] rawJsonMessage) {
		if (rawJsonMessage == null) {
			logger.warn("No bytes given for receiving the AdminMessage.", new NullPointerException());
			return;
		}
		try {
			receiveRawMessage(mapper.get().readTree(new String(rawJsonMessage)).asText());
		} catch (Exception e) {
			logger.warn("Error while de-serializing the received raw AdminMessage {} : {}", new String(rawJsonMessage),
					e.getMessage());
		}
	}

	@Override
  	public void sendRawMessage(String message) {
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