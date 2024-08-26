/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.hub.throttling;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.Assert.assertEquals;

import com.here.xyz.hub.rest.TestSpaceWithFeature;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class RequesterThrottlingIT extends TestSpaceWithFeature {
    private static final String SPACE_ID = "space1";
    private static final String HUB_ENDPOINT = "http://localhost:8080/hub";
    private static final HttpClient client = HttpClient.newHttpClient();
    private static final Logger logger = LogManager.getLogger();


    @BeforeClass
    public static void setupClass() {
        removeSpace(SPACE_ID);
        createSpaceWithCustomStorage(SPACE_ID, "client-throttling", null, 10);
        addFeatures(SPACE_ID);
    }

    @AfterClass
    public static void cleanUp() {
        removeSpace(SPACE_ID);
    }

    @Test
    public void testClientThrottling() throws InterruptedException {
        AtomicInteger successfulCount = new AtomicInteger(0);
        AtomicInteger tooManyRequestsCount = new AtomicInteger(0);
        AtomicInteger inFlightCount = new AtomicInteger(0);
        ExecutorService executorService = newFixedThreadPool(15);

        CompletableFuture<?>[] futures = IntStream.range(0, 15)
                .mapToObj(i -> CompletableFuture.runAsync(() ->
                        {
                            try {
                                inFlightCount.incrementAndGet();
                                HttpResponse<String> response = getIterateSpace(SPACE_ID, AuthProfile.ACCESS_ALL);
                                if (response.statusCode() >= 200 && response.statusCode() < 300) {
                                    successfulCount.incrementAndGet();
                                    logger.warn("200 throttling request: " + response.statusCode() + response.body());
                                    inFlightCount.decrementAndGet();
                                } else if (response.statusCode() == 429) {
                                    tooManyRequestsCount.incrementAndGet();
                                    logger.warn("429 throttling request: " + response.statusCode() + response.body());
                                    inFlightCount.decrementAndGet();
                                } else {
                                    logger.warn("Failed throttling request: " + response.statusCode() + response.body());
                                }
                            } catch (IOException | InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        , executorService))
                .toArray(CompletableFuture[]::new);

        Thread.sleep(1000);
        assertEquals(15 - tooManyRequestsCount.get(), inFlightCount.get());

        CompletableFuture.allOf(futures).join();
        assertEquals(5, tooManyRequestsCount.get());
        assertEquals(10, successfulCount.get());
    }

    private HttpResponse<String> getIterateSpace(String spaceId, AuthProfile profile) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(HUB_ENDPOINT + "/spaces/" + spaceId + "/iterate?limit=1000&skipCache=true"))
                .header("Accept", "application/json")
                .headers("Authorization", getAuthHeaders(profile).get("Authorization"))
                .GET()
                .build();

        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
