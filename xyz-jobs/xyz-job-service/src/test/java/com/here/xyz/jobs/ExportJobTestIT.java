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

import com.here.xyz.jobs.datasets.filters.SpatialFilter;
import com.here.xyz.models.geojson.coordinates.LinearRingCoordinates;
import com.here.xyz.models.geojson.coordinates.PolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
import com.here.xyz.models.geojson.implementation.Polygon;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Tag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ExportJobTestIT extends JobTest {
    private final int featureCount = 50;

    @BeforeEach
    public void setUp() {
        createSpace(new Space()
                        .withId(SPACE_ID)
                        .withVersionsToKeep(10)
                , false);
        putRandomFeatureCollectionToSpace(SPACE_ID, featureCount);
    }

    @Test
    public void simpleExportWithTag() throws Exception {
        String tagName = "tag1";
        createTag(SPACE_ID, new Tag().withId(tagName));

        putRandomFeatureCollectionToSpace(SPACE_ID, 5);
        putRandomFeatureCollectionToSpace(SPACE_ID, 5);

        Job exportJob = buildExportJob(generateJobId(), null, new Ref(tagName));
        createSelfRunningJob(exportJob);

        checkSucceededJob(exportJob, featureCount);
    }

    @Test
    public void simpleExport() throws Exception {
        Job exportJob = buildExportJob();
        createSelfRunningJob(exportJob);

        checkSucceededJob(exportJob, featureCount);
    }

    @Test
    public void simpleEmptyFilteredExport() throws Exception {
        PolygonCoordinates polygonCoordinates = new PolygonCoordinates();
        LinearRingCoordinates lrc = new LinearRingCoordinates();

        //No change is inside this polygon
        lrc.add(new Position(-15.346685180729708, 17.497068486524824));
        lrc.add(new Position(-15.346685180729708, 17.411220600917204));
        lrc.add(new Position(-15.167656664190616, 17.411220600917204));
        lrc.add(new Position(-15.167656664190616, 17.497068486524824));
        lrc.add(new Position(-15.346685180729708, 17.497068486524824));

        polygonCoordinates.add(lrc);

        Job exportJob = buildExportJob(generateJobId(), new Filters().withSpatialFilter(new SpatialFilter()
                .withGeometry(new Polygon().withCoordinates(polygonCoordinates))
                .withClip(false)));

        createSelfRunningJob(exportJob);
        checkSucceededJob(exportJob, 0);
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
    public void testRecreationOfSpaceExport() throws Exception {
        Job exportJob1 = buildExportJob();
        createSelfRunningJob(exportJob1);
        checkSucceededJob(exportJob1, featureCount);

        //recreate space which should break the reusability
        deleteSpace(SPACE_ID);
        createSpace(SPACE_ID);
        putRandomFeatureCollectionToSpace(SPACE_ID, featureCount);

        Job exportJob2 = buildExportJob(generateJobId());
        createSelfRunningJob(exportJob2);
        checkJobReusage(exportJob1, exportJob2, featureCount, false);
    }

    @Test
    public void testReusingSimpleExportMismatch() throws Exception {
        Job exportJob1 = buildExportJob();
        createSelfRunningJob(exportJob1);
        checkSucceededJob(exportJob1, featureCount);

        //Job could not reuse the first one
        Job exportJob2 = buildExportJob(generateJobId(), new Filters().withPropertyFilter("p.test<10"), null);
        createSelfRunningJob(exportJob2);
        checkJobReusage(exportJob1, exportJob2, 10, false);
    }

    private Job buildExportJob() {
        return buildExportJob(JOB_ID);
    }

    private Job buildExportJob(String jobId) {
        return buildExportJob(jobId, null, null);
    }

    private Job buildExportJob(String jobId, Filters filters) {
        return buildExportJob(jobId, filters, null);
    }

    private Job buildExportJob(String jobId, Filters filters, Ref ref) {
        return new Job()
                .withId(jobId)
                .withDescription("Export Job Test")
                .withSource(new DatasetDescription.Space<>().withId(SPACE_ID).withFilters(filters).withVersionRef(ref == null ? new Ref("HEAD") : ref))
                .withTarget(new Files<>().withOutputSettings(new FileOutputSettings().withFormat(new GeoJson().withEntityPerLine(Feature))));
    }
}
