package com.here.naksha.lib.extension;

import com.here.naksha.lib.core.IEventContext;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.models.hub.plugins.Connector;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.responses.HealthStatus;
import org.jetbrains.annotations.NotNull;

/**
 * A handler that the customer supplied and wish to deploy.
 */
public class RemoteCustomerHandler implements IEventHandler {

  public RemoteCustomerHandler(Connector connector) {}

  @Override
  public @NotNull XyzResponse processEvent(@NotNull IEventContext eventContext) {
    return new HealthStatus();
  }
}
