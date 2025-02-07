package com.here.xyz.jobs;

import static com.here.xyz.jobs.datasets.files.FileFormat.EntityPerLine.Feature;

import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.FileOutputSettings;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.datasets.files.GeoJson;
import com.here.xyz.jobs.datasets.filters.Filters;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ExportJobTestIT extends JobTest {
    private final int featureCount = 50;

    @BeforeEach
    public void setUp() {
        createSpace(SPACE_ID);
        putRandomFeatureCollectionToSpace(SPACE_ID, featureCount);
    }

    @Test
    public void simpleExport() throws Exception {
        Job exportJob = buildExportJob();
        createSelfRunningJob(exportJob);

        checkSucceededJob(exportJob, featureCount);
    }

    @Disabled("Potentially flickering: Ensure that 2nd job does not re-use 1st one")
    @Test
    public void twoSimpleExportsInParallel() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        List<Callable<Job>> tasks = List.of(
            () -> {
                Job exportJob = buildExportJob(generateJobId());
                createSelfRunningJob(exportJob);
                return exportJob;
            },
            () -> {
                Job exportJob = buildExportJob(generateJobId());
                createSelfRunningJob(exportJob);
                return exportJob;
            }
        );
        List<Future<Job>> futures = executorService.invokeAll(tasks);
        List<Job> jobs = new ArrayList<>();
        for (Future<Job> future : futures)
            jobs.add(future.get());

        checkSucceededJob(jobs.get(0), featureCount);
        checkSucceededJob(jobs.get(1), featureCount);

        executorService.shutdown();
    }

    @Test
    public void testReusingSimpleExportMatch() throws Exception {
        Job exportJob1 = buildExportJob();
        createSelfRunningJob(exportJob1);
        checkSucceededJob(exportJob1, featureCount);

        Job exportJob2 = buildExportJob(generateJobId());
        createSelfRunningJob(exportJob2);
        checkJobReusage(exportJob1, exportJob2, featureCount, true);

        //inactive cause we allow the deletion currently
        /**
        boolean exceptionRaised = false;
        try{
            deleteJob(exportJob1.getId());
        }catch (RuntimeException e){
            //Job can not get deleted due to references to job2
            exceptionRaised = true;
        }
        Assertions.assertTrue(exceptionRaised);*/
    }

    @Test
    public void testReusingSimpleExportMismatch() throws Exception {
        Job exportJob1 = buildExportJob();
        createSelfRunningJob(exportJob1);
        checkSucceededJob(exportJob1, featureCount);

        //Job could not reuse the first one
        Job exportJob2 = buildExportJob(generateJobId(), new Filters().withPropertyFilter("p.test<10"));
        createSelfRunningJob(exportJob2);
        checkJobReusage(exportJob1, exportJob2, 10, false);
    }

    private Job buildExportJob() {
        return buildExportJob(JOB_ID);
    }

    private Job buildExportJob(String jobId) {
        return buildExportJob(jobId, null);
    }

    private Job buildExportJob(String jobId, Filters filters) {
        return new Job()
                .withId(jobId)
                .withDescription("Export Job Test")
                .withSource(new DatasetDescription.Space<>().withId(SPACE_ID).withFilters(filters))
                .withTarget(new Files<>().withOutputSettings(new FileOutputSettings().withFormat(new GeoJson().withEntityPerLine(Feature))));
    }
}
