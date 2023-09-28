package com.here.xyz.httpconnector.util.jobs;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;

import com.here.xyz.httpconnector.config.JDBCClients;
import com.here.xyz.hub.rest.HttpException;
import io.vertx.core.Future;

public abstract class JDBCBasedJob<T extends JDBCBasedJob> extends Job<T> {

  @Override
  public Future<Job> executeAbort() {
    return super.executeAbort()
        //Job will fail - because SQL Queries are getting terminated
        .compose(job -> JDBCClients.abortJobsByJobId(this), e -> Future.failedFuture(new HttpException(BAD_GATEWAY, "Abort failed [" + getStatus() + "]")))
        .map(v -> this);
  }
}
