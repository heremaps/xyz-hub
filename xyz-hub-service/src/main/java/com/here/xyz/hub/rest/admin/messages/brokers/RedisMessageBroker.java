/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

import com.here.xyz.hub.Service;
import com.here.xyz.hub.rest.admin.MessageBroker;
import com.here.xyz.hub.rest.admin.Node;
import com.here.xyz.hub.util.RetryUtil;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.net.NetClientOptions;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisConnection;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Request;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RedisMessageBroker implements MessageBroker {

  private static final Logger logger = LogManager.getLogger();

  private Redis client;
  private Redis messageReceiverClient;
  private RedisConnection messageReceiverConnection;
  public static final String CHANNEL = "XYZ_HUB_ADMIN_MESSAGES";
  private static int MAX_MESSAGE_SIZE = 1024 * 1024;
  private static volatile RedisMessageBroker instance;
  private static int CONNECTION_KEEP_ALIVE = 60; //s

  public RedisMessageBroker() {
    try {
      client = Redis.createClient(Service.vertx, getClientConfig(true));
      messageReceiverClient = Redis.createClient(Service.vertx, getClientConfig(false));
    }
    catch (Exception e) {
      logger.error("Error while subscribing node in Redis.", e);
    }
  }

  private RedisOptions getClientConfig(boolean withIdleTimeout) {
    RedisOptions config = new RedisOptions()
        .setConnectionString(Service.configuration.getRedisUri())
        .setNetClientOptions(new NetClientOptions()
            .setTcpKeepAlive(true)
            .setIdleTimeout(withIdleTimeout ? CONNECTION_KEEP_ALIVE : 0)
            .setConnectTimeout(2000));

    //Use redis auth token when available
    if (Service.configuration.XYZ_HUB_REDIS_AUTH_TOKEN != null)
      config.setPassword(Service.configuration.XYZ_HUB_REDIS_AUTH_TOKEN);

    return config;
  }

  private synchronized Future<Void> connect() {
    Promise p = Promise.promise();
    if (messageReceiverConnection != null) {
      p.complete();
      return p.future();
    }
    messageReceiverClient.connect().onComplete(ar -> {
      messageReceiverConnection = ar.result();
      subscribeOwnNode(ar);
      p.complete();
    });
    return p.future();
  }

  public void fetchSubscriberCount(Handler<AsyncResult<Integer>> handler) {
    Request req = Request.cmd(Command.PUBSUB).arg("NUMSUB").arg(CHANNEL);
    if (client == null) {
      handler.handle(Future.failedFuture("No redis connection was established."));
      return;
    }
    client.send(req).onComplete(ar -> {
      if (ar.succeeded())
        //The 2nd array element contains the channel-subscriber count
        handler.handle(Future.succeededFuture(ar.result().get(1).toInteger()));
      else
        handler.handle(Future.failedFuture(ar.cause()));
    });
  }

  private void subscribeOwnNode(AsyncResult ar) {
    logger.info("Subscribing the NODE=" + Node.OWN_INSTANCE.getUrl());

    String errMsg = "The Node could not be subscribed as AdminMessage listener. No AdminMessages will be received by this node.";
    if (ar.succeeded()) {
      //That defines the handler for incoming messages on the connection
      messageReceiverConnection.handler(message -> {
        if (message.size() >= 3 && "message".equals(message.get(0).toString()) && CHANNEL.equals(message.get(1).toString())) {
          String rawMessage = message.get(2).toString();
          receiveRawMessage(rawMessage);
        }
      });
      Request req = Request.cmd(Command.SUBSCRIBE).arg(CHANNEL);
      messageReceiverConnection.send(req).onComplete(arSub -> {
        if (arSub.succeeded()) {
          logger.info("Subscription succeeded for NODE=" + Node.OWN_INSTANCE.getUrl());
        }
        else
          logger.error(errMsg, arSub.cause());
      });
    }
    else
      logger.error(errMsg, ar.cause());
  }

  @Override
  public void sendRawMessage(String jsonMessage) {
    if (client == null) {
      logger.warn("The AdminMessage can not be sent as the MessageBroker is not ready. Message was: {}", jsonMessage);
      return;
    }
    if (jsonMessage.length() > MAX_MESSAGE_SIZE) {
      throw new RuntimeException("AdminMessage is larger than the MAX_MESSAGE_SIZE. Can not send it.");
    }
    //Send using Redis client
    RetryUtil.<Void>executeWithRetry(task -> {
      Promise<Void> p = Promise.promise();
      Request req = Request.cmd(Command.PUBLISH).arg(CHANNEL).arg(jsonMessage);
      client.send(req).onComplete(ar -> {
        if (ar.succeeded())
          p.complete();
        else
          p.fail(ar.cause());
      });
      return p.future();
    }, t -> 50, 2, 5_000)
        .future()
        .onSuccess(v -> logger.debug("Message has been sent with following content: {}", jsonMessage))
        .onFailure(t -> logger.error("Error sending message: {}", jsonMessage, t));
  }

  public static Future<RedisMessageBroker> getInstance() {
    Promise<RedisMessageBroker> p = Promise.promise();
    if (instance != null) {
      p.complete(instance);
      return p.future();
    }
    instance = new RedisMessageBroker();
    instance.connect().onComplete(ar -> {
      if (ar.succeeded()) {
        logger.info("Subscribing node in Redis pub/sub channel {} succeeded.", CHANNEL);
        p.complete(instance);
      }
    });
    return p.future();
  }
}
