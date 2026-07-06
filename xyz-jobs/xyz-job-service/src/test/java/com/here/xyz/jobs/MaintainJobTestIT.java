/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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
package com.here.xyz.jobs;

import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.processes.Maintain;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.StatisticsResponse.PropertyStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MaintainJobTestIT extends JobTest {
    private static final Logger logger = LogManager.getLogger();

    static {
        XyzSerializable.registerSubtypes(Maintain.class);
    }

    @Test
    public void testMaintainSimpleSpace() throws Exception {
        //Indexes are created during the space creation
        createSpace(new Space().withId(SPACE_ID).withSearchableProperties(Map.of(
                        "foo1", true,
                        "foo2.nested", true,
                        "foo3.nested.array::array", true
                )
        ), false);

        //Modify the searchable properties
        patchSpace(SPACE_ID, Map.of( "searchableProperties", Map.of(
                        "foo1", true,
                        "foo2.nested", false,
                        "new", true
                ))
        );

        int maxAttempts = 10;
        findJobWaitAndDelete(SPACE_ID, maxAttempts);
        checkSearchableProperties(SPACE_ID, maxAttempts);
    }

    @Disabled //TODO: fix flickering test (can not be deleted as it is in state RUNNING)
    @Test
    public void testMaintainCompositeSpace() throws Exception {
        int amountOfExtensionPackages = 1;
        //Indexes are created during the space creation
        createSpace(new Space().withId(SPACE_ID).withSearchableProperties(Map.of(
                        "foo1", true,
                        "foo2.nested", true,
                        "foo3.nested.array::array", true
                )
        ), false);

        for (int i = 0; i < amountOfExtensionPackages; i++) {
            createSpace(new Space().withId(SPACE_ID + "-ext-" + i).withExtension(new Space.Extension().withSpaceId(SPACE_ID)), false);
            createSpace(new Space().withId(SPACE_ID + "-ext-ext-" + i).withExtension(new Space.Extension().withSpaceId(SPACE_ID + "-ext-" + i)), false);
        }

        //Modify the searchable properties
        patchSpace(SPACE_ID, Map.of( "searchableProperties", Map.of(
                        "foo1", true,
                        "foo2.nested", false,
                        "new", true
                ))
        );

        int maxAttempts = 10;

        findJobWaitAndDelete(SPACE_ID, maxAttempts);
        checkSearchableProperties(SPACE_ID, maxAttempts);

        for (int i = 0; i < amountOfExtensionPackages; i++) {
            findJobWaitAndDelete(SPACE_ID + "-ext-" + i, maxAttempts);
            findJobWaitAndDelete(SPACE_ID + "-ext-ext-" + i, maxAttempts);

            checkSearchableProperties(SPACE_ID + "-ext-" + i, maxAttempts);
            checkSearchableProperties(SPACE_ID + "-ext-ext-" + i, maxAttempts);
        }
    }

    private void findJobWaitAndDelete(String spaceId, int maxAttempts) throws InterruptedException, IOException {
        String jobId = null;

        while (jobId == null) {
            logger.info("Search job for space {}", spaceId);
            if(maxAttempts-- == 0)
                Assertions.fail("No job found after maximum attempts");
            try {
                List<Map> jobs = getJobsOnResource(spaceId, false);

                if(jobs != null && !jobs.isEmpty()) {
                    if(jobs.size() > 1) {
                        //if there is a L2 extension we will find multiple jobs on the intermediate spaces
                        jobId = jobs.stream().filter(j -> j.get("description").toString().endsWith(spaceId)).findFirst().get().get("id").toString();
                    } else
                        jobId = jobs.get(0).get("id").toString();
                    break;
                }
            } catch (RuntimeException | IOException e) {
                if(!e.getMessage().contains("404 response"))
                    throw e;
            }
            logger.info("Did not find job for space {}. Retrying...", spaceId);
            Thread.sleep(1000);
        }
        logger.info("Found job with id: {}", jobId);
        pollJobStatus(jobId);
        logger.info("Job {} on {} finished with state: {}", jobId, spaceId, getJobStatus(jobId).getState());
        deleteJob(jobId);
    }

    private void checkSearchableProperties(String spaceId, int maxAttempts) throws InterruptedException {
        Set<String> expectedSearchableProperties = new HashSet<>(Set.of("foo1", "new", "foo3.nested.array"));
        Set<String> foundSearchableProperties = new HashSet<>();

        while (!expectedSearchableProperties.equals(foundSearchableProperties)) {
            if(maxAttempts-- == 0)
                Assertions.fail("Wrong statistic found after maximum attempts");

            logger.info("{}: requesting statistics...", spaceId);
            StatisticsResponse statistics = getStatistics(spaceId, false);
            if(statistics == null)
                continue;

            List<PropertyStatistics> searchableProperties = statistics.getProperties().getValue();

            searchableProperties.forEach(property -> {
                if(expectedSearchableProperties.contains(property.getKey()))
                    foundSearchableProperties.add(property.getKey());
            });

            Thread.sleep(1000);
        }
    }
}
