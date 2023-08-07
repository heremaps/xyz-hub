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
package com.here.xyz.httpconnector.rest;

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.hub.rest.ApiParam;
import com.here.xyz.hub.rest.HttpException;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;

import java.util.HashMap;

import static com.here.xyz.hub.AbstractHttpServerVerticle.STREAM_INFO_CTX_KEY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

public class HApiParam extends ApiParam {

    public static class Path extends ApiParam.Path {
        public static final String JOB_ID = "jobId";
    }

    public static class HQuery extends Query{
        static final String ENABLED_HASHED_SPACE_ID = "enableHashedSpaceId";
        static final String TARGET_SPACEID = "targetSpaceId";
        public static final String FORCE = "force";
        public static final String H_COMMAND = "command";
        public static final String URL_COUNT = "urlCount";
        public static final String INCREMENTAL = "incremental";

        public enum Command {
            START,RETRY,ABORT,CREATEUPLOADURL;
            public static Command of(String value) {
                if (value == null) {
                    return null;
                }
                try {
                    return valueOf(value.toUpperCase());
                } catch (IllegalArgumentException e) {
                    return null;
                }
            }
        }

        public static Command getCommand(RoutingContext context) {
            Command command = Command.of(getString(context, "command", null));

            if(command == null)
                return null;

            switch (command){
                case START:
                case RETRY:
                case ABORT:
                case CREATEUPLOADURL:
                    return command;
                default:
                    return null;
            }
        }

        public static Job.Status getJobStatus(RoutingContext context) {
            Job.Status status = Job.Status.of(getString(context, "status", null));

            if(status == null)
                return null;

            switch (status){
                case aborted:
                case failed:
                case queued:
                case waiting:
                case executed:
                    return status;
                default:
                    return null;
            }
        }

        protected static Job.Type getJobType(RoutingContext context) {
            Job.Type type = Job.Type.of(getString(context, "type", null));

            if(type == null)
                return null;

            switch (type){
                case Export:
                case Import:
                    return type;
                default:
                    return null;
            }
        }

        public static Job getJobInput(final RoutingContext context) throws HttpException{
            try {
                Job job = Json.decodeValue(context.body().asString(), Job.class);
                return job;
            }
            catch (DecodeException e) {
                throw new HttpException(BAD_REQUEST, e.getMessage());
            }
        }

        protected static String[] parseMainParams(RoutingContext context) {
            String connectorId = Query.getString(context, "connectorId", null);
            addStreamInfo(context, "SID", connectorId);

            return new String[]{
                    connectorId,
                    Query.getString(context, "ecps", null),
                    Query.getString(context, "passphrase", CService.configuration.ECPS_PHRASE)
            };
        }

        protected static void addStreamInfo(final RoutingContext context, String key, Object value){
            context.put(STREAM_INFO_CTX_KEY, new HashMap<String, Object>(){{put(key, value);}});
        }

        public static Incremental getIncremental(RoutingContext context){
            return Incremental.of(Query.getString(context, INCREMENTAL, Incremental.DEACTIVATED.toString()).toUpperCase());
        }
    }
}
