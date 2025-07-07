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
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.processes.Maintain;
import com.here.xyz.models.hub.Space;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;

@Disabled
public class MaintainJobTestIT extends JobTest {
    @BeforeEach
    public void setUp() {
        XyzSerializable.registerSubtypes(Maintain.class);

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
    }

    @Test
    public void testSimpleMaintain() throws Exception {
        Job maintainJob = buildMaintainJob();
        createJob(maintainJob);
        createdJobs.add(maintainJob.getId());

        //Wait till Job reached final state
        pollJobStatus(maintainJob.getId());
        //TODO: check result
    }

    private Job buildMaintainJob() {
        return new Job()
                .withId(JOB_ID)
                .withDescription("Maintain Job Test")
                .withSource(new DatasetDescription.Space<>().withId(SPACE_ID))
                .withProcess(new Maintain());
    }
}
