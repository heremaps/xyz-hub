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

package com.here.xyz.events;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.events.ModifyBranchEvent.Operation;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ModifyBranchEventTest {

    private static ObjectMapper MAPPER;

    @BeforeAll
    static void setUpMapper() {
        MAPPER = new ObjectMapper()
                .findAndRegisterModules()
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    @Test
    void roundTripCreate() throws Exception {
        ModifyBranchEvent src = new ModifyBranchEvent()
                .withOperation(Operation.CREATE);

        String json = MAPPER.writeValueAsString(src);
        ModifyBranchEvent back = MAPPER.readValue(json, ModifyBranchEvent.class);

        assertEquals(Operation.CREATE, back.getOperation());
        assertEquals(0, back.getNodeId());
        assertNull(back.getBaseRef());
        assertNull(back.getNewBaseRef());
        assertEquals(-1, back.getMergeTargetNodeId());
    }

    @Test
    void roundTripMergeWithIds() throws Exception {
        ModifyBranchEvent src = new ModifyBranchEvent()
                .withOperation(Operation.MERGE)
                .withNodeId(123)
                .withMergeTargetNodeId(456);

        String json = MAPPER.writeValueAsString(src);
        ModifyBranchEvent back = MAPPER.readValue(json, ModifyBranchEvent.class);

        assertEquals(Operation.MERGE, back.getOperation());
        assertEquals(123, back.getNodeId());
        assertEquals(456, back.getMergeTargetNodeId());
        assertNull(back.getBaseRef());
        assertNull(back.getNewBaseRef());
    }
}
