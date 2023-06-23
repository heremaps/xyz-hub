package com.here.naksha.lib.extension;

import com.here.naksha.lib.core.IEventContext;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.models.hub.plugins.Connector;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.events.info.HealthCheckEvent;
import com.here.naksha.lib.core.models.payload.responses.HealthStatus;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

public class TestCustomerHandler {

  public class RemoteCustomerHandler implements IEventHandler {

    @Override
    public @NotNull XyzResponse processEvent(@NotNull IEventContext eventContext) {
      return new HealthStatus();
    }
  }

  @Test
  public void testRemote() throws IOException {
    final RemoteExtensionServer remoteExtensionServer = new RemoteExtensionServer(5435); //start the remote server on port
    remoteExtensionServer.start();

    final HealthCheckEvent event = new HealthCheckEvent();
    event.setConnector(new Connector("test", RemoteCustomerHandler.class));
  }
}
