/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

package com.here.xyz.httpconnector.util.jobs.validate;

import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.hub.Core;
import org.apache.commons.lang3.RandomStringUtils;

public class Validator {

    protected static void setJobDefaults(Job job){
        job.setCreatedAt(Core.currentTimeMillis() / 1000L);
        job.setUpdatedAt(Core.currentTimeMillis() / 1000L);

        if (job.getId() == null) {
            job.setId(RandomStringUtils.randomAlphanumeric(6));
        }

        if(job.getErrorType() != null){
            job.setErrorType(null);
        }
        if(job.getErrorDescription() != null){
            job.setErrorDescription(null);
        }
    }

    public static void validateJobCreation(Job job) throws Exception {
        if(job.getTargetSpaceId() == null){
            throw new Exception("Please specify 'targetSpaceId'!");
        }
        if(job.getCsvFormat() == null){
            throw new Exception("Please specify 'csvFormat'!");
        }
    }
}
