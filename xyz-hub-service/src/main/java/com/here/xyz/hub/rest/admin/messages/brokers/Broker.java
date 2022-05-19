package com.here.xyz.hub.rest.admin.messages.brokers;

import com.here.xyz.hub.rest.admin.MessageBroker;
import io.vertx.core.Future;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

/**
 * The enumeration about all possible message broker implementation. The name can be used in the
 * configuration file.
 */
public enum Broker {
  Redis(RedisMessageBroker.class, RedisMessageBroker::getInstance),
  Sns(SnsMessageBroker.class, SnsMessageBroker::getInstance),
  Noop(NoopBroker.class, NoopBroker::getInstance);

  Broker(
      final @Nonnull Class<? extends MessageBroker> brokerClass,
      final @Nonnull Supplier<Future<? extends MessageBroker>> instance
      ) {
    this.brokerClass = brokerClass;
    this.instance = instance;
  }

  /**
   * The class that implements this message broker.
   */
  @Nonnull
  public final Class<? extends MessageBroker> brokerClass;

  /**
   * A reference to the supplier that returns the broker singleton. If the broker is not yet
   * initialized, this will be done asynchronously.
   */
  @Nonnull
  public final Supplier<Future<? extends MessageBroker>> instance;

}
