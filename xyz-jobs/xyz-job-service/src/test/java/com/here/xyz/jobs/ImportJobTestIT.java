package com.here.xyz.jobs;

import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.datasets.files.FileInputSettings;
import com.here.xyz.jobs.datasets.files.GeoJson;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import com.here.xyz.jobs.util.test.ContentCreator;
import org.junit.jupiter.api.Test;

import static com.here.xyz.jobs.datasets.files.FileFormat.EntityPerLine.Feature;

public class ImportJobTestIT extends JobTest {


    //TODO: enable if JobService runs during tests
    //@Test
    public void testSimpleImport() throws Exception {
        Job importJob = buildImportJob();
        createAndStartJob(importJob, ContentCreator.generateImportFileContent(ImportFilesToSpace.Format.GEOJSON, 50));
        deleteJob(importJob.getId());
    }

    private Job buildImportJob() {
        return new Job()
                .withId(JOB_ID)
                .withDescription("Import Job Test")
                .withSource(new Files<>().withInputSettings(new FileInputSettings().withFormat(new GeoJson().withEntityPerLine(Feature))))
                .withTarget(new DatasetDescription.Space<>().withId(SPACE_ID));
    }
}
