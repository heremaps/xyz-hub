package com.here.xyz.hub.rest.admin.messages.brokers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.rest.AdminApi;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.rest.admin.MessageBroker;
import com.here.xyz.hub.rest.admin.Node;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.impl.ConnectionBase;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RedisMessageBroker implements MessageBroker {

  private static final Logger logger = LogManager.getLogger();

  private RedisOptions config;
  private RedisClient redis;
  public static final String CHANNEL = "XYZ_HUB_ADMIN_MESSAGES";
  private static int MAX_MESSAGE_SIZE = 1024 * 1024;
  private static volatile RedisMessageBroker instance;

  private List<String> hubRemoteUrls = null;

  public RedisMessageBroker() {
    try {
      config = new RedisOptions()
          .setHost(Service.configuration.XYZ_HUB_REDIS_HOST)
          .setPort(Service.configuration.XYZ_HUB_REDIS_PORT);
      config.setTcpKeepAlive(true);
      config.setConnectTimeout(2000);
      redis = RedisClient.create(Service.vertx, config);

      subscribeOwnNode(r -> {
        if (r.succeeded()) {
          logger.info("Subscribing node in Redis pub/sub channel {} succeeded.", CHANNEL);
        }
      });
    }
    catch (Exception e) {
      logger.error("Error while subscribing node in Redis.", e);
    }

    if (StringUtils.isEmpty(Service.configuration.XYZ_HUB_REMOTE_SERVICE_URLS)){
      hubRemoteUrls = Arrays.asList(Service.configuration.XYZ_HUB_REMOTE_SERVICE_URLS.split(";"));
    }
  }

  public void fetchSubscriberCount(Handler<AsyncResult<Integer>> handler) {
    redis.pubsubNumsub(Collections.singletonList(RedisMessageBroker.CHANNEL), r -> {
      if (r.succeeded())
        //The 2nd array element contains the channel-subscriber count
        handler.handle(Future.succeededFuture(r.result().getInteger(1)));
      else
        handler.handle(Future.failedFuture(r.cause()));
    });
  }

  private void subscribeOwnNode(Handler<AsyncResult<Void>> callback) {
    logger.info("Subscribing the NODE=" + Node.OWN_INSTANCE.getUrl());

    redis.subscribe(CHANNEL, ar -> {
      if (ar.succeeded()) {
        logger.info("Subscription succeeded for NODE=" + Node.OWN_INSTANCE.getUrl());
        Service.vertx.eventBus().consumer("io.vertx.redis." + CHANNEL, message -> {
          if (message instanceof Message && message.body() instanceof JsonObject) {
            receiveRawMessage(((JsonObject) message.body()).getJsonObject("value").getString("message"));
          }
        });
      }
      else if (ar.failed())
        logger.error("The Node could not be subscribed as AdminMessage listener. No AdminMessages will be received by this node.",
            ar.cause());
    });
  }

  @Override
  public void sendRawMessage(String jsonMessage) {
    if (redis == null) {
      logger.warn("The AdminMessage can not be sent as the MessageBroker is not ready. Message was: {}", jsonMessage);
      return;
    }
    if (jsonMessage.length() > MAX_MESSAGE_SIZE) {
      throw new RuntimeException("AdminMessage is larger than the MAX_MESSAGE_SIZE. Can not send it.");
    }
    //Send using Redis client
    redis.publish(CHANNEL, jsonMessage, ar -> {
      if (ar.succeeded())
        logger.debug("Message has been sent with following content: {}", jsonMessage);
      else
        logger.error("Error sending message: {}", jsonMessage, ar.cause());
    });

    sendRawMessagesToRemoteCluster(jsonMessage);
  }

  private void sendRawMessagesToRemoteCluster(String jsonMessage) {
    if (hubRemoteUrls != null) {

      for (String remoteUrl : hubRemoteUrls) {
        try {
          byte[] body = mapper.get().writeValueAsBytes(jsonMessage);

          int tryCount = 0;
          boolean retry = false;
          do {
            tryCount++;
            try {
              synchronized (Service.webClient) {
                Service.webClient
                        .postAbs(remoteUrl + AdminApi.ADMIN_MESSAGES_ENDPOINT)
                        .timeout(Service.configuration.REMOTE_FUNCTION_REQUEST_TIMEOUT)
                        .putHeader("content-type", "application/json; charset=" + Charset.defaultCharset().name())
                        .sendBuffer(Buffer.buffer(body), ar -> {
                          if (ar.failed()) {
                            logger.error("Failed to sent message to remote cluster. " + ar.cause());
                          }
                        });
              }
            } catch (Exception e) {
              if (!retry) {
                logger.error("Error sending event to remote http service. Retrying once...", e);
                retry = true;
              }
            }
          } while (retry && tryCount <= 1);

        } catch (JsonProcessingException e) {
          logger.error("Error while serializing AdminMessage prior to send it. AdminMessage: {}", jsonMessage);
        } catch (Exception e) {
          logger.error("Error while sending AdminMessage: {}", jsonMessage);
        }
      }
    }
  }

  public static synchronized RedisMessageBroker getInstance() {
    if (instance != null)
      return instance;
    return instance = new RedisMessageBroker();
  }
}
