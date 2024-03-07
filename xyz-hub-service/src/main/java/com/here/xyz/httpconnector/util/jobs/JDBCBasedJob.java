package com.here.xyz.httpconnector.util.jobs;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;

import com.here.xyz.httpconnector.task.StatusHandler;
import com.here.xyz.util.service.HttpException;
import io.vertx.core.Future;

public abstract class JDBCBasedJob<T extends JDBCBasedJob> extends Job<T> {

  @Override
  public Future<Job> executeAbort() {
    return super.executeAbort()
        //Job will fail - because SQL Queries are getting terminated
        .compose(job -> StatusHandler.getInstance().abortJob(this),
            e -> Future.failedFuture(new HttpException(BAD_GATEWAY, "Abort failed [" + getStatus() + "]")))
        .map(v -> this);
  }
}
