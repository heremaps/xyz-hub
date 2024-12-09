package com.here.xyz.jobs.steps.compiler;

import static com.here.xyz.jobs.datasets.files.FileFormat.EntityPerLine.Feature;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.JobTest;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.FileOutputSettings;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.datasets.files.GeoJson;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.util.service.BaseHttpServerVerticle;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ExportToFilesTest extends JobTest {

    @BeforeEach
    public void setUp() {
        createSpace(new Space()
                        .withId(SPACE_ID)
                        .withVersionsToKeep(10)
                , false);

        putRandomFeatureCollectionToSpace(SPACE_ID, 2);
        putRandomFeatureCollectionToSpace(SPACE_ID, 2);
        putRandomFeatureCollectionToSpace(SPACE_ID, 2);
    }

    @Test
    public void testResolveHeadVersion() throws BaseHttpServerVerticle.ValidationException {
        CompilationStepGraph graph = new ExportToFiles().compile(buildExportJobWithVersionRef(new Ref("HEAD")));
        final ExportSpaceToFiles exportSpaceToFilesStep = getAndPrepareStep(graph);
        //HEAD should point to version 3
        Assertions.assertEquals(3, exportSpaceToFilesStep.getVersionRef().getVersion());
    }

    private static ExportSpaceToFiles getAndPrepareStep(CompilationStepGraph graph) throws BaseHttpServerVerticle.ValidationException {
        final ExportSpaceToFiles exportSpaceToFilesStep = (ExportSpaceToFiles) graph.getExecutions().get(0);
        exportSpaceToFilesStep.prepare(null, null);
        return exportSpaceToFilesStep;
    }

    @Test
    public void testResolveNotExistingTag(){
        CompilationStepGraph graph = new ExportToFiles().compile(buildExportJobWithVersionRef(new Ref("NA")));
        //NA not exists - should fail
        Assertions.assertThrows(IllegalArgumentException.class, () -> getAndPrepareStep(graph));
    }

    @Test
    public void testResolveExistingTag() throws BaseHttpServerVerticle.ValidationException {
        String tagName = "TAG1";
        int tagVersion = 2;

        createTag(SPACE_ID, new Tag().withId(tagName).withVersion(tagVersion));

        CompilationStepGraph graph = new ExportToFiles().compile(buildExportJobWithVersionRef(new Ref(tagName)));
        Assertions.assertEquals(tagVersion, getAndPrepareStep(graph).getVersionRef().getVersion());
    }

    private Job buildExportJobWithVersionRef(Ref versionRef) {
        return new Job()
                .withId(JOB_ID)
                .withDescription("Export Job Test")
                .withSource(new DatasetDescription.Space<>().withId(SPACE_ID).withVersionRef(versionRef))
                .withTarget(new Files<>().withOutputSettings(new FileOutputSettings().withFormat(new GeoJson().withEntityPerLine(Feature))));
    }
}
