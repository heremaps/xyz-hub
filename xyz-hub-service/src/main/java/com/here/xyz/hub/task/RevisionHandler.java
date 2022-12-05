package com.here.xyz.hub.task;

import com.here.xyz.events.RevisionEvent;
import io.vertx.core.Future;

public class RevisionHandler {

  public static Future<Void> execute(RevisionEvent e) {
    return Future.succeededFuture();
  }
}
