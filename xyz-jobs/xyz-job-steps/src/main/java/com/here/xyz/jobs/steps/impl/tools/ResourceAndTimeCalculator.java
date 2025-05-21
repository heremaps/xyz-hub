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

package com.here.xyz.jobs.steps.impl.tools;

import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper.Index;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper.OnDemandIndex;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper.SystemIndex;
import com.here.xyz.util.di.ImplementationProvider;
import com.here.xyz.util.service.Initializable;

public class ResourceAndTimeCalculator implements Initializable {
    private static int MIN_IMPORT_TIME_IN_SECONDS = 30 * 60;
    private static int MAX_IMPORT_TIME_IN_SECONDS = 30 * 60 * 60;

    private static int MIN_IDX_CREATION_TIME_IN_SECONDS = 60 * 60;
    private static int MAX_IDX_CREATION_TIME_IN_SECONDS = 10 * 60 * 60;

    public static ResourceAndTimeCalculator getInstance() {
        return ResourceAndTimeCalculator.Provider.provideInstance();
    }

    public static class Provider implements ImplementationProvider{

        protected ResourceAndTimeCalculator getInstance() {
            return new ResourceAndTimeCalculator();
        }

        private static ResourceAndTimeCalculator provideInstance() {
            return ImplementationProvider.loadProvider(ResourceAndTimeCalculator.Provider.class).getInstance();
        }

        @Override
        public boolean chooseMe() {
            return (Config.instance != null && Config.instance.APP_NAME == null);
        }
    }

    //Import Related...
    protected double importTimeFactor(String spaceId, double bytesPerBillion){
        return 0.44 * bytesPerBillion;
    }

    public int calculateImportTimeInSeconds(String spaceId, long byteSize, LambdaBasedStep.ExecutionMode executionMode){
        int warmUpTime = 10;
        double bytesPerBillion = byteSize / 1_000_000_000d;
        int totalTime = (int)(warmUpTime + importTimeFactor(spaceId, bytesPerBillion) * 60);

        if(executionMode.equals(LambdaBasedStep.ExecutionMode.SYNC)) {
            totalTime *= 2;
        }
        return totalTime;
    }

    public int calculateImportTimeoutSeconds(String spaceId, long byteSize, LambdaBasedStep.ExecutionMode executionMode) {
        int t = calculateImportTimeInSeconds(spaceId, byteSize, executionMode) * 3;
        return Math.min(Math.max(t, MIN_IMPORT_TIME_IN_SECONDS), MAX_IMPORT_TIME_IN_SECONDS);
    }

    public double calculateNeededImportAcus(long uncompressedUploadBytesEstimation, int fileCount, int threadCount) {
        //maximum auf Acus - to prevent that job never gets executed. @TODO: check how to deal is maxUnits of DB
        final double maxAcus = 70;
        final long bytesPerThreads;

        if (fileCount == 0)
            fileCount = 1;

        //Only take into account the max parallel execution
        bytesPerThreads = uncompressedUploadBytesEstimation / fileCount * threadCount;

        //Calculate the needed ACUs
        double neededAcus = threadCount * calculateNeededAcusFromByteSize(bytesPerThreads);
        return Math.min(neededAcus, maxAcus);
    }

    public int calculateNeededImportDBThreadCount(long uncompressedUploadBytesEstimation, int fileCount, int maxDbThreadCount) {
        int calculatedThreadCount;
        //1GB for maxThreads
        long uncompressedByteSizeForMaxThreads = 1024L * 1024 * 1024;

        if (uncompressedUploadBytesEstimation >= uncompressedByteSizeForMaxThreads)
            calculatedThreadCount = maxDbThreadCount;
        else {
            // Calculate linearly scaled thread count
            int threadCnt = (int) ((double) uncompressedUploadBytesEstimation / uncompressedByteSizeForMaxThreads * maxDbThreadCount);
            calculatedThreadCount = threadCnt == 0 ? 1 : threadCnt;
        }

        return Math.min(calculatedThreadCount, fileCount == 0 ? 1 : fileCount);
    }

    public double calculateNeededAcusFromByteSize(long byteSize) {
        //maximum auf Acus - to prevent that job never gets executed. @TODO: check how to deal is maxUnits of DB
        final double maxAcus = 70;
        // Each ACU needs 2GB RAM
        final double GB_TO_BYTES = 1024 * 1024 * 1024;
        final int ACU_RAM = 2; // GB

        //Calculate the needed ACUs
        double requiredRAM = byteSize / GB_TO_BYTES;
        double neededAcus = requiredRAM / ACU_RAM;

        return Math.min(neededAcus, maxAcus);
    }

    //Index Related...
    protected double geoIndexFactor(String spaceId, double bytesPerBillion){
        return  0.091 * bytesPerBillion;
    }

    public int calculateIndexCreationTimeInSeconds(String spaceId, long byteSize, Index index){
        int warmUpTime = 1;
        double bytesPerBillion = byteSize  / 1_000_000_000d;
        double fact = byteSize < 100_000_000_000l ? 0.5 : 1;
        if(index == null)
            return 1;

        double importTimeInMin = 0;

        if(index instanceof OnDemandIndex)
            importTimeInMin = 0.014 * bytesPerBillion;
        else if(index instanceof SystemIndex systemIndex) {
            importTimeInMin = switch (systemIndex) {
                case GEO -> geoIndexFactor(spaceId, bytesPerBillion);
                case VERSION_ID -> 0.014 * bytesPerBillion;
                case VIZ -> 0.025 * bytesPerBillion;
                case OPERATION -> 0.012 * bytesPerBillion;
                case NEXT_VERSION -> 0.013 * bytesPerBillion;
                case AUTHOR -> 0.013 * bytesPerBillion;
                case SERIAL -> 0.013 * bytesPerBillion;
            };
        }

        return (int)(warmUpTime + (importTimeInMin * fact * 60));
    }

    public double calculateNeededIndexAcus(long byteSize, Index index) {
        double minACUs = 0.01;
        //Threshold which defines when we scale to maximum
        double globalMax = 200d * 1024 * 1024 * 1024;

        if(index instanceof OnDemandIndex)
            return interpolate(globalMax, 10, byteSize, minACUs) / 4d; //TODO: Remove workaround once GraphSequentializer was implemented
        else if(index instanceof SystemIndex systemIndex) {
            return switch (systemIndex) {
                case GEO -> interpolate(globalMax, 30, byteSize, minACUs);
                case VIZ -> interpolate(globalMax, 10, byteSize, minACUs);
                case VERSION_ID -> interpolate(globalMax, 10, byteSize, minACUs);
                case OPERATION -> interpolate(globalMax, 10, byteSize, minACUs);
                case NEXT_VERSION -> interpolate(globalMax, 10, byteSize, minACUs);
                case AUTHOR -> interpolate(globalMax, 10, byteSize, minACUs);
                case SERIAL -> interpolate(globalMax, 8, byteSize, minACUs);
            } / 4d; //TODO: Remove workaround once GraphSequentializer was implemented
        }
        return 0;
    }

    public int calculateIndexTimeoutSeconds(String spaceId, long byteSize, Index index) {
        int t = calculateIndexCreationTimeInSeconds(spaceId, byteSize, index) * 3;
        return Math.min(Math.max(t, MIN_IDX_CREATION_TIME_IN_SECONDS), MAX_IDX_CREATION_TIME_IN_SECONDS);
    }

    private static double interpolate(double globalMax, double max, long real, double min){
        if(real >= globalMax)
            return max;
        else{
            double interpolated = (real / globalMax) * max;
            return Math.max(interpolated, min);
        }
    }
}
