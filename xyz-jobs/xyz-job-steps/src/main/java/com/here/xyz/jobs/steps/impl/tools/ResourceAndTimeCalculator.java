package com.here.xyz.jobs.steps.impl.tools;

import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.execution.db.DatabaseBasedStep;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper;
import com.here.xyz.util.di.ImplementationProvider;
import com.here.xyz.util.service.Initializable;

public class ResourceAndTimeCalculator implements Initializable {
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
        if(executionMode.equals(LambdaBasedStep.ExecutionMode.ASYNC)) {
            int warmUpTime = 10;
            double bytesPerBillion = byteSize / 1_000_000_000d;
            return (int) (warmUpTime + importTimeFactor(spaceId, bytesPerBillion) * 60);
        }else{
            int expectedHubThroughPutBytesPerSec = 800_000;
            int overhead = 2;
            return (int) (byteSize / expectedHubThroughPutBytesPerSec * overhead);
        }
    }

    public double calculateNeededImportAcus(long uncompressedUploadBytesEstimation, int fileCount, int threadCount) {
        //maximum auf Acus - to prevent that job never gets executed. @TODO: check how to deal is maxUnits of DB
        final double maxAcus = 70;
        // Each ACU needs 2GB RAM
        final double GB_TO_BYTES = 1024 * 1024 * 1024;
        final int ACU_RAM = 2; // GB
        final long bytesPerThreads;

        if (fileCount == 0)
            return 0;

        //Only take into account the max parallel execution
        bytesPerThreads = uncompressedUploadBytesEstimation / fileCount * threadCount;

        //RDS processing of 9,5GB zipped leads into ~120 GB RDS Mem
        //Calculate the needed ACUs
        double requiredRAMPerThreads = bytesPerThreads / GB_TO_BYTES;
        double neededAcus = threadCount * requiredRAMPerThreads / ACU_RAM;

        return neededAcus > maxAcus ? maxAcus : neededAcus;
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

        return calculatedThreadCount > fileCount ? fileCount : calculatedThreadCount;
    }

    // Index Related...
    protected double geoIndexFactor(String spaceId, double bytesPerBillion){
        return  0.091 * bytesPerBillion;
    }

    public int calculateIndexCreationTimeInSeconds(String spaceId, long byteSize, XyzSpaceTableHelper.Index index){
        int warmUpTime = 1;
        double bytesPerBillion = byteSize  / 1_000_000_000d;
        double fact = byteSize < 100_000_000_000l ? 0.5 : 1;
        if(index == null)
            return 1;

        double importTimeInMin =  switch (index){
            case GEO -> geoIndexFactor(spaceId, bytesPerBillion);
            case CREATED_AT ->  0.064 * bytesPerBillion;
            case UPDATED_AT ->  0.064 * bytesPerBillion;
            case ID_VERSION ->  0.063 * bytesPerBillion;
            case ID ->  0.063 * bytesPerBillion;
            case VERSION ->  0.014 * bytesPerBillion;
            case VIZ ->  0.025 * bytesPerBillion;
            case OPERATION ->  0.012 * bytesPerBillion;
            case NEXT_VERSION ->  0.013 * bytesPerBillion;
            case AUTHOR ->  0.013 * bytesPerBillion;
            case SERIAL ->  0.013 * bytesPerBillion;
        };

        return (int)(warmUpTime + (importTimeInMin * fact * 60));
    }

    public double calculateNeededIndexAcus(long byteSize, XyzSpaceTableHelper.Index index) {
        double minACUs = 0.01;
        //Threshold which defines when we scale to maximum
        double globalMax = 200d * 1024 * 1024 * 1024;

        return switch (index){
            case GEO -> interpolate(globalMax,30, byteSize, minACUs);
            case CREATED_AT ->  interpolate(globalMax, 25, byteSize, minACUs);
            case UPDATED_AT ->  interpolate(globalMax, 25, byteSize, minACUs);
            case ID_VERSION ->  interpolate(globalMax, 25, byteSize, minACUs);
            case ID ->  interpolate(globalMax, 20, byteSize, minACUs);
            case VIZ ->  interpolate(globalMax,10, byteSize, minACUs);
            case VERSION ->  interpolate(globalMax,  10, byteSize, minACUs);
            case OPERATION ->  interpolate(globalMax,10, byteSize, minACUs);
            case NEXT_VERSION ->  interpolate(globalMax, 10, byteSize, minACUs);
            case AUTHOR ->  interpolate(globalMax,10, byteSize, minACUs);
            case SERIAL ->  interpolate(globalMax, 8, byteSize, minACUs);
        };
    }

    public int calculateIndexTimeoutSeconds(long byteSize, XyzSpaceTableHelper.Index index) {
        int minTimeoutInSeconds = 6 * 60;
        //Threshold which defines when we scale to maximum
        double globalMax = 400d * 1024 * 1024 * 1024;

        if(index == null)
            return minTimeoutInSeconds;

        return switch (index){
            case GEO -> (int)interpolate(globalMax, (4+2) * 3600, byteSize, minTimeoutInSeconds);
            case CREATED_AT ->  (int)interpolate(globalMax, (2+1) * 3600, byteSize, minTimeoutInSeconds);
            case UPDATED_AT ->  (int)interpolate(globalMax, (2+1) * 3600, byteSize, minTimeoutInSeconds);
            case ID_VERSION ->  (int)interpolate(globalMax, (2+1) * 3600, byteSize, minTimeoutInSeconds);
            case ID ->  (int)interpolate(globalMax, (2+1) * 3600, byteSize, minTimeoutInSeconds);
            case VIZ ->  (int)interpolate(globalMax, (1 + 1) * 3600, byteSize, minTimeoutInSeconds);
            case VERSION ->  (int)interpolate(globalMax,  (0.5 + 0.5) * 3600, byteSize, minTimeoutInSeconds);
            case OPERATION ->  (int)interpolate(globalMax, (0.5 + 0.5) * 3600, byteSize, minTimeoutInSeconds);
            case NEXT_VERSION ->  (int)interpolate(globalMax, (0.5 + 0.5) * 3600, byteSize, minTimeoutInSeconds);
            case AUTHOR ->  (int)interpolate(globalMax, (0.5 + 0.5) * 3600, byteSize, minTimeoutInSeconds);
            case SERIAL ->  (int)interpolate(globalMax, (0.4 + 0.4) * 3600, byteSize, minTimeoutInSeconds);
        };
    }

    private static double interpolate(double globalMax, double max, long real, double min){
        if(real >= globalMax)
            return max;
        else{
            double interpolated = (real / globalMax) * max;
            return interpolated < min ? min : interpolated;
        }
    }
}
