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
import com.here.xyz.hub.rest.HttpException;
import org.apache.commons.lang3.RandomStringUtils;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.PRECONDITION_FAILED;

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

    public static void validateJobCreation(Job job) throws HttpException {
        if(job.getTargetSpaceId() == null){
            throw new HttpException(BAD_REQUEST,("Please specify 'targetSpaceId'!"));
        }
        if(job.getCsvFormat() == null){
            throw new HttpException(BAD_REQUEST,("Please specify 'csvFormat'!"));
        }
    }

    protected static void isValidForStart(Job job) throws HttpException{
        switch (job.getStatus()){
            case finalized:
                throw new HttpException(PRECONDITION_FAILED, "Job is already finalized !");
            case failed:
                throw new HttpException(PRECONDITION_FAILED, "Failed - check error and retry!");
            case queued:
            case validating:
            case validated:
            case preparing:
            case prepared:
            case executing:
            case executed:
            case executing_trigger:
            case trigger_executed:
            case collectiong_trigger_status:
            case trigger_status_collected:
            case finalizing:
                throw new HttpException(PRECONDITION_FAILED, "Job is already running - current status: "+job.getStatus());
        }
    }
}
