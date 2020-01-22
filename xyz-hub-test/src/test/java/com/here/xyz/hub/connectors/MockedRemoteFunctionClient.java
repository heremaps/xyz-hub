/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */

package com.here.xyz.hub.connectors;

import com.here.xyz.hub.connectors.models.Connector;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class MockedRemoteFunctionClient extends RemoteFunctionClient {

    private static final Logger logger = LogManager.getLogger();
    ScheduledThreadPoolExecutor threadPool;
    private long minExecutionTime = 0; //ms
    private long maxExecutionTime = 30_000; //ms

    public MockedRemoteFunctionClient(Connector connectorConfig) {
        super(connectorConfig);
        threadPool = new ScheduledThreadPoolExecutor(connectorConfig.connectionSettings.maxConnections, Executors.defaultThreadFactory(), (r, executor) -> {
            MockedRequest mr = (MockedRequest) r;
            mr.callback.handle(Future.failedFuture(new Exception("Connector is too busy.")));
        });
        threadPool.setMaximumPoolSize(connectorConfig.connectionSettings.maxConnections);
    }

    public MockedRemoteFunctionClient(Connector connectorConfig, long minExecutionTime, long maxExecutionTime) {
        this(connectorConfig);
        this.minExecutionTime = minExecutionTime;
        this.maxExecutionTime = maxExecutionTime;
    }

    public MockedRemoteFunctionClient(Connector connectorConfig, long executionTime) {
        this(connectorConfig, executionTime, executionTime);
    }

    @Override
    protected void invoke(Marker marker, byte[] bytes, Handler<AsyncResult<byte[]>> callback) {
        long executionTime = (long) (Math.random() * (double) (maxExecutionTime - minExecutionTime) + minExecutionTime);

        MockedRequest req = new MockedRequest(callback) {
            @Override
            public void run() {
                try {
                    Thread.sleep(executionTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                endTime = System.currentTimeMillis();
                long eT = this.endTime - this.startTime;
                logger.info("Request " + requestId + " was executed with desired executionTime: " + executionTime + "ms and actual eT: " + eT + "ms; relEndTime: " + (endTime - testStart));
                callback.handle(Future.succeededFuture());
            }
        };

        threadPool.schedule(req, 0, TimeUnit.MILLISECONDS);
    }


    public static abstract class MockedRequest implements Runnable {
        Handler<AsyncResult<byte[]>> callback;
        String requestId = UUID.randomUUID().toString();
        long startTime = System.currentTimeMillis();
        long endTime;
        public static long testStart;

        MockedRequest(Handler<AsyncResult<byte[]>> callback) {
            this.callback = callback;
        }
    }
}
