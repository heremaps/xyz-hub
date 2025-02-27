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
import com.here.xyz.httpconnector.CService;
import com.here.xyz.util.di.ImplementationProvider;

import java.util.List;

public class EMRManager {
    private AWSEMRServerlessClient emrClient;

    public EMRManager() {
        emrClient = (AWSEMRServerlessClient) AWSEMRServerlessClient.builder().standard()
            .withRegion(Regions.fromName(CService.configuration.JOBS_REGION))
            .build();
    }
    public static EMRManager getInstance() {
        return EMRManager.Provider.provideInstance();
    }

    public static class Provider implements ImplementationProvider {

        protected EMRManager getInstance() {
            return new EMRManager();
        }

        private static EMRManager provideInstance() {
            EMRManager emrManager =  ImplementationProvider.loadProvider(EMRManager.Provider.class).getInstance();
            return emrManager;
        }

        @Override
        public boolean chooseMe() {
            return !"test".equals(System.getProperty("scope"));
        }
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

    public String getExecutionSummary(String applicationId, String jobRunId) {
        GetJobRunRequest getJobRunRequest = new GetJobRunRequest()
                .withApplicationId(applicationId)
                .withJobRunId(jobRunId);

        GetJobRunResult jobRunResult = emrClient.getJobRun(getJobRunRequest);
        return jobRunResult.getJobRun().getState();
    }

    public void shutdown(String applicationId, String jobRunId) {
        CancelJobRunRequest cancelJobRunRequest = new CancelJobRunRequest()
                .withApplicationId(applicationId)
                .withJobRunId(jobRunId);

        CancelJobRunResult cancelJobRunResult = emrClient.cancelJobRun(cancelJobRunRequest);
    }
}
