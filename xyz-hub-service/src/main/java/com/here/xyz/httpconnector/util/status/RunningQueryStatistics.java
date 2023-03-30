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

import java.util.ArrayList;
import java.util.List;

public class RunningQueryStatistics{
    private List<RunningQueryStatistic> runningQueryStatisticList;
    private long importBytesInProgress;
    private int runningIndexQueries;
    private int runningImports;
    private int runningS3Exports;
    private int runningVMLExports;

    public RunningQueryStatistics(){
        runningIndexQueries = 0;
        runningImports = 0;
        runningVMLExports = 0;
        runningS3Exports = 0;
        runningQueryStatisticList = new ArrayList<>();
    }

    public void addRunningQueryStatistic(RunningQueryStatistic runningQueryStatistic){
        this.runningQueryStatisticList.add(runningQueryStatistic);
        switch (runningQueryStatistic.queryType){
            case IMPORT_S3:
                importBytesInProgress += runningQueryStatistic.getBytesInProgress();
                runningImports++;
                break;
            case IMPORT_IDX:
                runningIndexQueries++;
                break;
            case EXPORT_S3:
                runningS3Exports++;
                break;
            case EXPORT_VML:
                runningVMLExports++;
                break;
        }
    }

    public List<RunningQueryStatistic> getRunningQueries() {
        return runningQueryStatisticList;
    }

    public long getImportBytesInProgress() {
        return importBytesInProgress;
    }

    public int getRunningIndexQueries() {
        return runningIndexQueries;
    }

    public int getRunningImports() {
        return runningImports;
    }

    public int getRunningS3Exports() {
        return runningS3Exports;
    }

    public int getRunningVMLExports() {
        return runningVMLExports;
    }
}
