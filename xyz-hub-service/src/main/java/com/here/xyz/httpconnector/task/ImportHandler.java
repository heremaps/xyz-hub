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

package com.here.xyz.httpconnector.task;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.PRECONDITION_FAILED;

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.web.HubWebClient;
import com.here.xyz.hub.rest.HttpException;
import io.vertx.core.Future;
import org.apache.logging.log4j.Marker;

public class ImportHandler extends JobHandler {

    protected static Future<Job> postJob(Import job, Marker marker){
        try {
            job.setDefaults();
            job.validate();
        }
        catch (Exception e) {
            return Future.failedFuture(new HttpException(BAD_REQUEST, e.getMessage()));
        }

        return HubWebClient.getSpaceStatistics(job.getTargetSpaceId())
                .compose(statistics -> {
                    Long value = statistics.getCount().getValue();
                    if (value != null && value != 0)
                        return Future.failedFuture(new HttpException(PRECONDITION_FAILED, "Layer is not empty!"));
                    return CService.jobConfigClient.store(marker, job);
                });
    }
}
