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
package com.here.xyz.jobs.steps.compiler.tools;

import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.JobCompiler;
import com.here.xyz.jobs.steps.impl.CreateIndex;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.db.pg.IndexHelper;
import com.here.xyz.util.db.pg.IndexHelper.OnDemandIndex;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient;

import java.util.List;
import java.util.Map;

public class IndexCompilerHelper {
    /**
     * Creates a CompilationStepGraph for the on-demand indices of the given space.
     * @param spaceId The ID of the space for which to create the on-demand indices.
     * @return A CompilationStepGraph containing the steps to create the on-demand indices, which are getting executed in parallel.
     * @throws JobCompiler.CompilationError If an error occurs while retrieving the searchable properties.
     */
    public static CompilationStepGraph compileOnDemandIndexSteps(String spaceId) throws JobCompiler.CompilationError {

        List<OnDemandIndex> activatedSearchableProperties = getActiveSearchableProperties(spaceId);
        CompilationStepGraph onDemandIndicesGraph = (CompilationStepGraph) new CompilationStepGraph().withParallel(true);

        for(OnDemandIndex index : activatedSearchableProperties) {
            // Create an OnDemandIndex step for each activated searchable property
            onDemandIndicesGraph.addExecution(new CreateIndex()
                    .withIndex(index)
                    .withSpaceId(spaceId));
        }
        return onDemandIndicesGraph;
    }

    /**
     * Retrieves the names of active searchable properties for the given space ID.
     * @param spaceId The ID of the space for which to retrieve the searchable property names.
     * @return A list containing the names of the active searchable properties.
     * @throws JobCompiler.CompilationError If an error occurs while retrieving the searchable properties.
     */
    public static List<OnDemandIndex> getActiveSearchableProperties(String spaceId) throws JobCompiler.CompilationError {
        try {

            Space space = HubWebClient.getInstance(Config.instance.HUB_ENDPOINT)
                    .loadSpace(spaceId);
            Map<String, Boolean> searchableProperties = space.getSearchableProperties();

            if(space.getExtension() != null){
                return getActiveSearchableProperties(space.getExtension().getSpaceId());
            }

            return IndexHelper.getActivatedSearchableProperties(searchableProperties);
        }
        catch (XyzWebClient.WebClientException e) {
            throw new JobCompiler.CompilationError("Error fetching the searchable properties. Target is not accessible! " + e.getMessage(), e);
        }
    }
}
