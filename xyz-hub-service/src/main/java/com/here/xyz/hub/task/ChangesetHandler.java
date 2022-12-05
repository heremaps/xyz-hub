package com.here.xyz.hub.task;

import com.here.xyz.events.ChangesetEvent;
import io.vertx.core.Future;

public class ChangesetHandler {

  public static Future<Void> execute(ChangesetEvent e) {
    return Future.succeededFuture();
  }
}
