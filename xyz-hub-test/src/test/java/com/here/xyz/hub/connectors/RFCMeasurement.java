/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

import static org.junit.Assert.assertEquals;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.Service.Config;
import com.here.xyz.hub.connectors.models.Connector;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unused")
public class RFCMeasurement {

    MockedRemoteFunctionClient rfc;
    ScheduledThreadPoolExecutor requesterPool;

    public static long TEST_START;

    private static final int RFC_MAX_CONNECTIONS = 8;

    @Before
    public void setup() {
        //Mock necessary configuration values
        Service.configuration = new Config();
        Service.configuration.REMOTE_FUNCTION_REQUEST_TIMEOUT = 26;
        Service.configuration.INSTANCE_COUNT = 1;

        Connector s = new Connector();
        TEST_START = Service.currentTimeMillis();
        MockedRemoteFunctionClient.MockedRequest.testStart = TEST_START;
        s.id = "testStorage";
        s.connectionSettings = new Connector.ConnectionSettings();
        s.connectionSettings.maxConnections = RFC_MAX_CONNECTIONS;
        rfc = new MockedRemoteFunctionClient(s, 10);

        requesterPool = new ScheduledThreadPoolExecutor(20);
    }

    @After
    public void tearDown() {
        rfc.threadPool.shutdownNow();
        rfc = null;

        requesterPool.shutdownNow();
        requesterPool = null;
    }

    public void checkMeasuring(int concurrency, long offset, long interval, long measureDelay, int expectedArrivalRate,
                               int expectedThroughput) throws InterruptedException {
        ScheduledFuture<?> f = requesterPool.scheduleAtFixedRate(() -> {
            for (int i = 0; i < concurrency; i++) {
                long now = Service.currentTimeMillis();
                rfc.submit(null, null, false, r -> {
                    //Nothing to do
                });
            }
        }, offset, interval, TimeUnit.MILLISECONDS);

        Thread.sleep(measureDelay);

        double ar = rfc.getArrivalRate();
        double tp = rfc.getThroughput();


        assertEquals("arrivalRate should match", expectedArrivalRate, Math.round(ar));
        assertEquals("throughput should match", expectedThroughput, Math.round(tp));
    }

    @Test
    public void checkMeasuringBelowLimits() throws InterruptedException {
        checkMeasuring(4, 200, 1000, 1300, 4, 4);
    }

    //TODO: Write more tests
    //@Test
    public void checkMeasuringExceedConcurrencyLimit() throws InterruptedException {
        checkMeasuring(10,200, 1000, 1300, 10, RFC_MAX_CONNECTIONS);
    }

}
