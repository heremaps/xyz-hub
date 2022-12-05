/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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
package com.here.xyz.httpconnector.util.status;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class RunningQueryStatistic {
    private static String IDX_QUERY_IDENTIFIER = "index if not";
    private static String IMPORT_QUERY_IDENTIFIER = "iml_import_hint";

    private int pid;
    private String state;
    private String queryStart;
    private String stateChange;
    private String query;
    private String applicationName;
    private String duration;
    private long bytesInProgress;
    protected RunningQueryStatistic.QueryType queryType;

    public enum QueryType {
        IMPORT_IDX, IMPORT_S3, OTHER;

        public static QueryType of(String value) {
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

    public RunningQueryStatistic(int pid, String state, LocalDateTime queryStart, LocalDateTime stateChange, String query, String applicationName) {
        this.pid = pid;
        this.state = state;
        this.queryStart = queryStart.toString();
        this.stateChange = stateChange.toString();
        this.query = query;
        this.applicationName = applicationName;

        Duration dur = Duration.between(queryStart, LocalDateTime.now(ZoneOffset.UTC));
        long days = dur.toDays();
        dur = dur.minusDays(days);
        long hours = dur.toHours();
        dur = dur.minusHours(hours);
        long minutes = dur.toMinutes();
        dur = dur.minusMinutes(minutes);
        long seconds = dur.getSeconds() ;
        this.duration = (days ==  0? "" : days+" days,")+
                (hours == 0 ? "" : hours+" hours,")+
                (minutes ==  0 ? "" : minutes+" minutes,")+
                (seconds == 0 ? "" : seconds+" seconds");

        if(this.query != null) {
            try {
                if (this.query.toLowerCase().indexOf(IDX_QUERY_IDENTIFIER) != -1) {
                    this.queryType = RunningQueryStatistic.QueryType.IMPORT_IDX;
                    this.query = query.substring(query.indexOf("idx_") + 4, query.indexOf(" ON ") - 1);
                }else if (this.query.toLowerCase().indexOf(IMPORT_QUERY_IDENTIFIER) != -1) {
                    this.queryType = RunningQueryStatistic.QueryType.IMPORT_S3;
                    this.query = query.substring(query.indexOf(")),'") + 4, query.indexOf("as " + IMPORT_QUERY_IDENTIFIER) - 2);
                    this.bytesInProgress = Long.parseLong(this.query.substring(this.query.indexOf(":") + 1));
                    this.query = this.query.substring(0,this.query.indexOf(":"));
                }else
                    this.queryType = RunningQueryStatistic.QueryType.OTHER;
            } catch (Exception e) {}
        }
    }

    public int getPid() {
        return pid;
    }

    public String getState() {
        return state;
    }

    public String getQueryStart() {
        return queryStart;
    }

    public String getStateChange() {
        return stateChange;
    }

    public String getQuery() {
        return query;
    }

    public RunningQueryStatistic.QueryType getQueryType() {
        return queryType;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public String getDuration() { return duration; }

    public Long getBytesInProgress() { return bytesInProgress; }

    @Override
    public String toString() {
        return "RunningQueryStatistic{" +
                "pid=" + pid +
                ", state='" + state + '\'' +
                ", queryStart='" + queryStart + '\'' +
                ", stateChange='" + stateChange + '\'' +
                ", query='" + query + '\'' +
                ", applicationName='" + applicationName + '\'' +
                ", duration='" + duration + '\'' +
                ", queryType=" + queryType +
                '}';
    }
}
