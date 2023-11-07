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

package com.here.xyz.httpconnector.util.emr;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.emrserverless.AWSEMRServerlessClient;
import com.amazonaws.services.emrserverless.model.CancelJobRunRequest;
import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunRequest;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobDriver;
import com.amazonaws.services.emrserverless.model.JobRunState;
import com.amazonaws.services.emrserverless.model.SparkSubmit;
import com.amazonaws.services.emrserverless.model.StartJobRunRequest;
import com.amazonaws.services.emrserverless.model.StartJobRunResult;
import java.util.List;

public class EMRManager {
    private AWSEMRServerlessClient emrClient;

    public EMRManager() {
        emrClient = (AWSEMRServerlessClient) AWSEMRServerlessClient.builder().standard()
            .withRegion(Regions.EU_WEST_1)
            .build();
    }

    public String startJob(String applicationId, String jobName, String runtimeRoleARN, String jarUrl, List<String> scriptParams, String sparkParams) {
        SparkSubmit sparkSubmit = new SparkSubmit()
                .withEntryPoint(jarUrl)
                .withEntryPointArguments(scriptParams)
                .withSparkSubmitParameters(sparkParams);
        JobDriver jobDriver = new JobDriver()
                .withSparkSubmit(sparkSubmit);
        StartJobRunRequest jobRunRequest = new StartJobRunRequest()
                .withName(jobName)
                .withApplicationId(applicationId)
                .withExecutionRoleArn(runtimeRoleARN)
                .withJobDriver(jobDriver);
        //TODO: Use ProgressListener
        StartJobRunResult jobRunResult = emrClient.startJobRun(jobRunRequest);
        return jobRunResult.getJobRunId();
    }

    public JobRunState getExecutionSummary(String applicationId, String jobRunId) {
        GetJobRunRequest getJobRunRequest = new GetJobRunRequest()
                .withApplicationId(applicationId)
                .withJobRunId(jobRunId);

        GetJobRunResult jobRunResult = emrClient.getJobRun(getJobRunRequest);
        return JobRunState.fromValue(jobRunResult.getJobRun().getState());
    }

    public void shutdown(String applicationId, String jobRunId) {
        CancelJobRunRequest cancelJobRunRequest = new CancelJobRunRequest()
                .withApplicationId(applicationId)
                .withJobRunId(jobRunId);

        CancelJobRunResult cancelJobRunResult = emrClient.cancelJobRun(cancelJobRunRequest);
    }
}
