package com.here.xyz.hub.rest.admin.messages.brokers;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.rest.admin.MessageBroker;
import com.here.xyz.hub.rest.admin.Node;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RedisMessageBroker implements MessageBroker {

  private static final Logger logger = LogManager.getLogger();

  private RedisOptions config;
  private Redis redis;
  private RedisConnection redisConnection;
  public static final String CHANNEL = "XYZ_HUB_ADMIN_MESSAGES";
  private static int MAX_MESSAGE_SIZE = 1024 * 1024;
  private static volatile RedisMessageBroker instance;

  public RedisMessageBroker() {
    try {
      config = new RedisOptions()
          .setConnectionString(Service.configuration.XYZ_HUB_REDIS_URI)
          .setNetClientOptions(new NetClientOptions()
              .setTcpKeepAlive(true)
              .setIdleTimeout(30)
              .setConnectTimeout(2000));

      //Use redis auth token when available
      if (!StringUtils.isEmpty(Service.configuration.XYZ_HUB_REDIS_AUTH_TOKEN))
        config.setPassword(Service.configuration.XYZ_HUB_REDIS_AUTH_TOKEN);

      redis = Redis.createClient(Service.vertx, config);
    }
    catch (Exception e) {
      logger.error("Error while subscribing node in Redis.", e);
    }
  }

  private synchronized Future<Void> connect() {
    Promise p = Promise.promise();
    if (redisConnection != null) {
      p.complete();
      return p.future();
    }
    redis.connect().onComplete(ar -> {
      redisConnection = ar.result();
      subscribeOwnNode(ar);
      p.complete();
    });
    return p.future();
  }

  public void fetchSubscriberCount(Handler<AsyncResult<Integer>> handler) {
    Request req = Request.cmd(Command.PUBSUB).arg("NUMSUB").arg(CHANNEL);
    redisConnection.send(req).onComplete(ar -> {
      if (ar.succeeded())
        //The 2nd array element contains the channel-subscriber count
        handler.handle(Future.succeededFuture(ar.result().get(1).toInteger()));
      else
        handler.handle(Future.failedFuture(ar.cause()));
    });
  }

  private void subscribeOwnNode(AsyncResult ar) {
    logger.info("Subscribing the NODE=" + Node.OWN_INSTANCE.getUrl());

    if (ar.succeeded()) {
      logger.info("Subscription succeeded for NODE=" + Node.OWN_INSTANCE.getUrl());
      //That defines the handler for incoming messages on the connection
      redisConnection.handler(message -> {
        receiveRawMessage(message.toString());
      });
    }
    else if (ar.failed())
      logger.error("The Node could not be subscribed as AdminMessage listener. No AdminMessages will be received by this node.",
          ar.cause());
  }

  @Override
  public void sendRawMessage(String jsonMessage) {
    if (redisConnection == null) {
      logger.warn("The AdminMessage can not be sent as the MessageBroker is not ready. Message was: {}", jsonMessage);
      return;
    }
    if (jsonMessage.length() > MAX_MESSAGE_SIZE) {
      throw new RuntimeException("AdminMessage is larger than the MAX_MESSAGE_SIZE. Can not send it.");
    }
    //Send using Redis client
    Request req = Request.cmd(Command.PUBLISH).arg(CHANNEL).arg(jsonMessage);
    redisConnection.send(req).onComplete(ar -> {
      if (ar.succeeded())
        logger.debug("Message has been sent with following content: {}", jsonMessage);
      else
        logger.error("Error sending message: {}", jsonMessage, ar.cause());
    });
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
