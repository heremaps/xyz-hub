package com.here.xyz.jobs;

import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.FileOutputSettings;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.datasets.files.GeoJson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;

import static com.here.xyz.jobs.datasets.files.FileFormat.EntityPerLine.Feature;

public class ExportJobTestIT extends JobTest {

    @BeforeEach
    public void setUp() throws IOException {
        createSpace(SPACE_ID);
        putRandomFeatureCollectionToSpace(SPACE_ID,50);
    }

    @AfterEach
    public void tearDown() {
        deleteSpace(SPACE_ID);
    }
//TODO: enable if JobService runs during tests
//    @Test
    public void testSimpleExport() throws Exception {
        Job exportJob = buildExportJob();
        createSelfRunningJob(exportJob);

        checkSucceededJob(exportJob);
        deleteJob(exportJob.getId());
    }

    private Job buildExportJob() {
        return new Job()
                .withId(JOB_ID)
                .withDescription("Export Job Test")
                .withSource(new DatasetDescription.Space<>().withId(SPACE_ID))
                .withTarget(new Files<>().withOutputSettings(new FileOutputSettings().withFormat(new GeoJson().withEntityPerLine(Feature))));
    }
}
