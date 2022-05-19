package com.here.xyz.hub.rest.admin.messages.brokers;

import com.here.xyz.hub.rest.admin.AdminMessage;
import com.here.xyz.hub.rest.admin.MessageBroker;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import javax.annotation.Nonnull;

/**
 * The Noop message broker is a local message broker, it will only work for single instances.
 */
public class NoopBroker implements MessageBroker {

  static Future<NoopBroker> getInstance() {
    final Promise<NoopBroker> promise = Promise.promise();
    promise.complete(new NoopBroker());
    return promise.future();
  }

  @Override
  public void sendRawMessage(String jsonMessage) {

  }

  @Override
  public void sendMessage(AdminMessage message) {
    MessageBroker.super.sendMessage(message);
  }

  @Override
  public void sendRawMessagesToRemoteCluster(String jsonMessage, int tryCount) {
    MessageBroker.super.sendRawMessagesToRemoteCluster(jsonMessage, tryCount);
  }

  @Override
  public void receiveRawMessage(byte[] rawJsonMessage) {
    MessageBroker.super.receiveRawMessage(rawJsonMessage);
  }

  @Override
  public void receiveRawMessage(String jsonMessage) {
    MessageBroker.super.receiveRawMessage(jsonMessage);
  }

  @Override
  public AdminMessage deserializeMessage(String jsonMessage) {
    return MessageBroker.super.deserializeMessage(jsonMessage);
  }

  @Override
  public void receiveMessage(AdminMessage message) {
    MessageBroker.super.receiveMessage(message);
  }

  @Nonnull
  @Override
  public Future<Integer> fetchSubscriberCount() {
    final Promise<Integer> promise = Promise.promise();
    promise.complete(1);
    return promise.future();
  }
}
