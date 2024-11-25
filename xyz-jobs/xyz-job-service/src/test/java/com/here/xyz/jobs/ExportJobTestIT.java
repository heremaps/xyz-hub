package com.here.xyz.jobs;

import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.FileOutputSettings;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.datasets.files.GeoJson;
import com.here.xyz.jobs.datasets.filters.Filters;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.here.xyz.jobs.datasets.files.FileFormat.EntityPerLine.Feature;

public class ExportJobTestIT extends JobTest {
    private final int featureCount = 50;

    @BeforeEach
    public void setUp() {
        super.setUp();
        putRandomFeatureCollectionToSpace(SPACE_ID, featureCount);
//        patchSpace(SPACE_ID, Map.of("readOnly", true));
    }

    @Test
    public void testSimpleExport() throws Exception {
        Job exportJob = buildExportJob();
        createSelfRunningJob(exportJob);

        checkSucceededJob(exportJob, featureCount);
        deleteJob(exportJob.getId());
    }

    @Test
    public void testReusingSimpleExportMatch() throws Exception {
        Job exportJob1 = buildExportJob();
        createSelfRunningJob(exportJob1);
        checkSucceededJob(exportJob1, featureCount);

        Job exportJob2 = buildExportJob(generateJobId());
        createSelfRunningJob(exportJob2);
        checkJobReusage(exportJob1, exportJob2, featureCount, true);

        boolean exceptionRaised = false;
        try{
            deleteJob(exportJob1.getId());
        }catch (RuntimeException e){
            //Job can not get deleted due to references to job2
            exceptionRaised = true;
        }
        Assertions.assertTrue(exceptionRaised);

        //delete in correct order
        deleteJob(exportJob2.getId());
        deleteJob(exportJob1.getId());
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

        deleteJob(exportJob1.getId());
        deleteJob(exportJob2.getId());
    }

    private Job buildExportJob() {
        return buildExportJob(JOB_ID);
    }

    private Job buildExportJob(String jobId){
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
