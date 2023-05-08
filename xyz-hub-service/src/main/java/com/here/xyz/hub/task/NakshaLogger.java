package com.here.xyz.hub.task;

import com.here.xyz.TaskLogger;
import io.vertx.ext.web.RoutingContext;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

/**
 * A special logger that only forwards logs to SLF4j, but ensures that the logs are all formatted the same way.
 */
public final class NakshaLogger extends TaskLogger implements Logger {

  public NakshaLogger(@NotNull RoutingContext routingContext) {
    this.streamId = NakshaTask.getStreamId(routingContext);
  }

  public NakshaLogger(@NotNull String streamId) {
    this.streamId = streamId;
  }

  private final @NotNull String streamId;

  @Override
  public @NotNull String streamId() {
    return streamId;
  }
}