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

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sns.AmazonSNSAsync;
import com.amazonaws.services.sns.AmazonSNSAsyncClientBuilder;
import com.amazonaws.services.sns.message.DefaultSnsMessageHandler;
import com.amazonaws.services.sns.message.SnsMessageManager;
import com.amazonaws.services.sns.message.SnsNotification;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicRequest;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicResult;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sns.model.Subscription;
import com.amazonaws.services.sns.model.UnsubscribeRequest;
import com.amazonaws.services.sns.model.UnsubscribeResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.rest.AdminApi;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.rest.admin.AdminMessage;
import com.here.xyz.hub.rest.admin.MessageBroker;
import com.here.xyz.hub.rest.admin.Node;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.io.ByteArrayInputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The MessageBroker provides the infrastructural implementation of how to send & receive {@link AdminMessage}s. Currently it's an
 * implementation relying purely on AWS SNS as a broadcasting mechanism.
 */
public class SnsMessageBroker extends DefaultSnsMessageHandler implements MessageBroker {

  private static final Logger logger = LogManager.getLogger();

  private static SnsMessageBroker instance;
  private static final ThreadLocal<ObjectMapper> mapper = ThreadLocal.withInitial(ObjectMapper::new);
  private static final long MAX_MESSAGE_SIZE = 256 * 1024;
  private static final String OWN_NODE_MESSAGING_URL;
  private static final String SNS_HTTP_PROTOCOL = "http";
  private static final String PENDING_CONFIRMATION = "PendingConfirmation";

  static {
    String ownNodeUrl;
    if (Service.configuration.ADMIN_MESSAGE_JWT == null) {
      ownNodeUrl = null;
    }
    else {
      try {
        ownNodeUrl = Node.OWN_INSTANCE.getUrl() != null ? new URL(Node.OWN_INSTANCE.getUrl(), AdminApi.ADMIN_MESSAGES_ENDPOINT
            + "?" + Query.ACCESS_TOKEN + "=" + Service.configuration.ADMIN_MESSAGE_JWT).toString() : null;
      }
      catch (MalformedURLException e) {
        logger.error("Error creating the Node URL", e);
        ownNodeUrl = null;
      }
    }
    OWN_NODE_MESSAGING_URL = ownNodeUrl;
  }

  private final String TOPIC_ARN;
  private final AmazonSNSAsync SNS_CLIENT;
  private final SnsMessageManager MESSAGE_MANAGER;
  private ScheduledExecutorService threadPool = new ScheduledThreadPoolExecutor(1);
  private List<Subscription> oldSubscriptions;

  private SnsMessageBroker() {
    logger.info("Initializing SnsMessageBroker");

    final String initWarnMsg = "Environment variable \"ADMIN_MESSAGE_TOPIC_ARN\" not defined. The node could not be subscribed as"
        + " AdminMessage listener. No AdminMessages will be received from SNS by this node.";
    String topicArn = null;
    SnsMessageManager messageManager = null;
    AmazonSNSAsync snsClient = null;

    if (Service.configuration.ADMIN_MESSAGE_TOPIC_ARN != null) {
      try {
        topicArn = Service.configuration.ADMIN_MESSAGE_TOPIC_ARN.toString();
        messageManager = new SnsMessageManager(Service.configuration.ADMIN_MESSAGE_TOPIC_ARN.getRegion());
        snsClient = AmazonSNSAsyncClientBuilder
            .standard()
            .withRegion(Service.configuration.ADMIN_MESSAGE_TOPIC_ARN.getRegion())
            .build();
      }
      catch (Exception e) {
        logger.warn(initWarnMsg, e);
        topicArn = null;
        messageManager = null;
        snsClient = null;
      }
    }
    else {
      logger.warn(initWarnMsg);
    }

    TOPIC_ARN = topicArn;
    MESSAGE_MANAGER = messageManager;
    SNS_CLIENT = snsClient;

    logger.debug("TOPIC_ARN resolved as: " + TOPIC_ARN);
    if (TOPIC_ARN == null || MESSAGE_MANAGER == null || SNS_CLIENT == null)
      return;

    try {
      subscribeOwnNode(r -> {
        if (r.succeeded()) {
          cleanup();
        }
      });
    }
    catch (Exception e) {
      logger.error("Error while subscribing node in SNS.", e);
    }
  }

  public static void preventSubscriptionCleanup() {
    SnsMessageBroker mb = getInstance();
    if (mb != null)
      mb.shutdownCleanup();
  }

  static SnsMessageBroker getInstance() {
    if (instance != null)
      return instance;
    return instance = new SnsMessageBroker();
  }

  @Override
  public void handle(SnsNotification message) {
    receiveMessage(message.getMessage());
  }

  private void subscribeOwnNode(Handler<AsyncResult<Void>> callback) {
    logger.info("Subscribing the NODE=" + Node.OWN_INSTANCE.getUrl());

    final String subscriptionErrorMsg = "The Node could not be subscribed as AdminMessage listener. "
        + "No AdminMessages will be received by this node.";
    if (OWN_NODE_MESSAGING_URL == null)
      throw new NullPointerException("No messaging node URL provided. " + subscriptionErrorMsg);

    //First check whether there is an existing subscription for the own endpoint
    loadSubscriptions(subscriptionsResult -> {
      if (subscriptionsResult.succeeded()) {
        oldSubscriptions = subscriptionsResult.result();
        logger.info("Subscriptions have been loaded [" + oldSubscriptions.size() + "] for NODE=" + Node.OWN_INSTANCE.getUrl());
        /*
        Check whether a subscription to the own node's endpoint already exists.
        (could happen by accident when this node re-uses an IP from a previously running node)
        */
        if (oldSubscriptions.stream().noneMatch(subscription -> OWN_NODE_MESSAGING_URL.equals(subscription.getEndpoint()))) {
          logger
              .info("Current node is not subscribed yet, subscribing NODE=" + Node.OWN_INSTANCE.getUrl() + " into TOPIC_ARN=" + TOPIC_ARN);
          SNS_CLIENT
              .subscribeAsync(TOPIC_ARN, SNS_HTTP_PROTOCOL, OWN_NODE_MESSAGING_URL, new AsyncHandler<SubscribeRequest, SubscribeResult>() {
                @Override
                public void onError(Exception e) {
                  logger.error(subscriptionErrorMsg, e);
                  callback.handle(Future.failedFuture(e));
                }

                @Override
                public void onSuccess(SubscribeRequest request, SubscribeResult subscribeResult) {
                  logger.info("Subscription succeeded for NODE=" + Node.OWN_INSTANCE.getUrl() + " into TOPIC_ARN=" + TOPIC_ARN);
                  callback.handle(Future.succeededFuture());
                }
              });
        }
      }
      else {
        logger.error(subscriptionErrorMsg, subscriptionsResult.cause());
      }
    });
  }

  private void cleanup() {
    long randomSeconds = (long) (Math.random() * (double) TimeUnit.MINUTES.toSeconds(Node.count()));
    long deferringSeconds = TimeUnit.MINUTES.toSeconds(10) + randomSeconds;
    long shutdownDelay = deferringSeconds + TimeUnit.MINUTES.toSeconds(1);

    if (oldSubscriptions == null) {
      logger.error("Cleanup of old AdminMessage subscriptions could not be performed.");
    }
    else {
      threadPool.schedule(() -> {
        if (oldSubscriptions != null) {
          //Cleanup old SNS subscriptions here. Do this deferred by 10 minutes + some random time
          oldSubscriptions.forEach(this::checkSubscription);
          //Send a notification to all other nodes to not care about the subscription cleanup anymore.
          new PreventSubscriptionCleanup().broadcast();
        }
      }, deferringSeconds, TimeUnit.SECONDS);
    }

    //Shut down and remove the pool once everything is done
    threadPool.schedule(this::shutdownCleanup, shutdownDelay, TimeUnit.SECONDS);
  }

  private void shutdownCleanup() {
    ScheduledExecutorService tp = threadPool;
    if (tp != null) {
      threadPool.shutdownNow();
      threadPool = null;
    }
    oldSubscriptions = null;
  }

  /**
   * Loads all subscriptions being relevant for the message broker. This includes only those using the HTTP protocol and pointing to an
   * admin messaging endpoint.
   */
  private void loadSubscriptions(Handler<AsyncResult<List<Subscription>>> callback) {
    loadSubscriptions(null, callback);
  }

  private void loadSubscriptions(String nextToken, Handler<AsyncResult<List<Subscription>>> callback) {
    ListSubscriptionsByTopicRequest req = new ListSubscriptionsByTopicRequest()
        .withTopicArn(TOPIC_ARN);
    if (nextToken != null) {
      req.setNextToken(nextToken);
    }
    SNS_CLIENT.listSubscriptionsByTopicAsync(req,
        new AsyncHandler<ListSubscriptionsByTopicRequest, ListSubscriptionsByTopicResult>() {
          @Override
          public void onError(Exception e) {
            callback.handle(Future.failedFuture(e));
          }

          @Override
          public void onSuccess(ListSubscriptionsByTopicRequest request, ListSubscriptionsByTopicResult result) {
            List<Subscription> subscriptions = result.getSubscriptions()
                .stream()
                .filter(subscription -> SNS_HTTP_PROTOCOL.equals(subscription.getProtocol())
                    && subscription.getEndpoint().contains(AdminApi.ADMIN_MESSAGES_ENDPOINT))
                .collect(Collectors.toCollection(LinkedList::new));
            if (result.getNextToken() != null) {
              loadSubscriptions(result.getNextToken(), subscriptionsResult -> {
                if (subscriptionsResult.succeeded()) {
                  subscriptions.addAll(subscriptionsResult.result());
                  callback.handle(Future.succeededFuture(subscriptions));
                } else {
                  callback.handle(subscriptionsResult);
                }
              });
            }
            else {
              callback.handle(Future.succeededFuture(subscriptions));
            }
          }
        });
  }

  private void checkSubscription(Subscription subscription) {
    String endpoint = subscription.getEndpoint();
    try {
      URL subscriptionUrl = new URL(endpoint);
      Node subscribedNode = Node.forIpAndPort(subscriptionUrl.getHost(), subscriptionUrl.getPort());
      subscribedNode.isAlive(ar -> {
        if (ar.failed()) {
          unsubscribe(subscription);
        }
      });
    }
    catch (MalformedURLException e) {
      logger.error("Subscription with endpoint {} could not be verified. It will be kept for now.", endpoint);
    }
  }

  private void unsubscribe(Subscription subscription) {
    if (PENDING_CONFIRMATION.equals(subscription.getSubscriptionArn())) {
      logger.warn("Could not remove subscription ({}) because it's pending for confirmation. It will be removed automatically "
          + "after 3 days.", subscription.getEndpoint());
      return;
    }
    SNS_CLIENT.unsubscribeAsync(subscription.getSubscriptionArn(), new AsyncHandler<UnsubscribeRequest, UnsubscribeResult>() {
      @Override
      public void onError(Exception e) {
        logger.error("Error un-subscribing endpoint {} from SNS: {}", subscription.getEndpoint(), e);
      }

      @Override
      public void onSuccess(UnsubscribeRequest request, UnsubscribeResult unsubscribeResult) {
        logger.debug("Endpoint {} has been successfully un-subscribed from SNS.", subscription.getEndpoint());
      }
    });
  }

  @Override
  public void sendRawMessage(String message) {
    if (SNS_CLIENT == null) {
      logger.warn("The AdminMessage can not be sent as the MessageBroker is not ready. Message was: {}", message);
      return;
    }
    if (message.length() > MAX_MESSAGE_SIZE) {
      throw new RuntimeException("AdminMessage is larger than the MAX_MESSAGE_SIZE. Can not send it.");
    }
    //Send using SNS client
    SNS_CLIENT.publishAsync(TOPIC_ARN, message, new AsyncHandler<PublishRequest, PublishResult>() {
      @Override
      public void onError(Exception exception) {
        logger.error("Error sending message: {}", message, exception);
      }

      @Override
      public void onSuccess(PublishRequest request, PublishResult publishResult) {
        logger.debug("Message has been sent with following content: {}", message);
      }
    });
  }

  @Override
  public void receiveRawMessage(byte[] rawJsonMessage) {
    if (rawJsonMessage == null) {
      logger.error("No bytes given for receiving the message.", new NullPointerException());
      return;
    }

    if (MESSAGE_MANAGER != null) {
      //The SNS message manager parses & verifies the incoming SNS message and calls the #handle() method.
      MESSAGE_MANAGER.handleMessage(new ByteArrayInputStream(rawJsonMessage), this);
    }
    else {
      //In case there is no actual SNS subscription we accept serialized AdminMessages directly (e.g. for testing purposes)
      receiveMessage(new String(rawJsonMessage));
    }
  }

  void receiveMessage(String jsonMessage) {
    receiveMessage(deserializeMessage(jsonMessage));
  }

  /**
   * An infrastructural message needed by the {@link SnsMessageBroker} to inform all other nodes that the subscription-cleanup was already
   * done by this node and can be skipped on the other ones.
   */
  public static class PreventSubscriptionCleanup extends AdminMessage {

    @Override
    protected void handle() {
      preventSubscriptionCleanup();
    }
  }
}
