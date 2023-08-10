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

import com.here.xyz.httpconnector.rest.HApiParam;
import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.rest.ApiParam;
import com.here.xyz.hub.rest.HttpException;
import org.apache.commons.lang3.RandomStringUtils;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.PRECONDITION_FAILED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;

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

    public static void isValidForExecution(Job job, HApiParam.HQuery.Command command, ApiParam.Query.Incremental incremental) throws HttpException {
        switch (command){
            case CREATEUPLOADURL:
                if(job instanceof Export)
                    throw new HttpException(NOT_IMPLEMENTED, "For Export not available!");
                else if(job instanceof Import)
                    ImportValidator.isValidForCreateUrl(job);
                break;
            case RETRY:
                    isValidForRetry(job);
                break;
            case START:
                if(job instanceof Export)
                    ExportValidator.isValidForStart((Export) job, incremental);
                else if(job instanceof Import)
                    ImportValidator.isValidForStart(job);
                break;
            case ABORT:
                if(job instanceof Export)
                    ExportValidator.isValidForAbort(job);
                else if(job instanceof Import)
                    ImportValidator.isValidForAbort(job);
        }
    }

    protected static void isValidForRetry(Job job) throws HttpException{
        if(!job.getStatus().equals(Job.Status.failed) && !job.getStatus().equals(Job.Status.aborted) )
            throw new HttpException(PRECONDITION_FAILED, "Invalid state: "+job.getStatus() +" for retry!");
    }

    public static boolean isValidForDelete(Job job, boolean force) {
        if(force)
            return true;
        switch (job.getStatus()){
            case waiting: case finalized: case aborted: case failed: return true;
            default: return false;
        }
    }
}
