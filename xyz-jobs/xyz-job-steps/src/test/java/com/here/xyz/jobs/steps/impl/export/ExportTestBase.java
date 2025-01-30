package com.here.xyz.jobs.steps.impl.export;

import static com.here.xyz.jobs.steps.Step.Visibility.SYSTEM;

import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.jobs.datasets.filters.SpatialFilter;
import com.here.xyz.jobs.steps.impl.StepTest;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Ref;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;

public class ExportTestBase extends StepTest {

    protected void executeExportStepAndCheckResults(String spaceId, ContextAwareEvent.SpaceContext context,
                                                    SpatialFilter spatialFilter, PropertiesQuery propertiesQuery,
                                                    Ref versionRef,
                                                    String hubPathAndQuery)
            throws IOException, InterruptedException {
        //Retrieve all Features from Space
        FeatureCollection allExpectedFeatures = customReadFeaturesQuery(spaceId, hubPathAndQuery);

        //Create Step definition
        ExportSpaceToFiles step = new ExportSpaceToFiles()
            .withSpaceId(spaceId)
            .withJobId(JOB_ID);

        if(context != null)
            step.setContext(context);
        if(propertiesQuery != null)
            step.setPropertyFilter(propertiesQuery);
        if(spaceId != null)
            step.setSpatialFilter(spatialFilter);
        if(versionRef != null)
            step.setVersionRef(versionRef);

        //Send Lambda Requests
        sendLambdaStepRequestBlock(step, true);
        checkOutputs(allExpectedFeatures, step.loadUserOutputs(), step.loadOutputs(SYSTEM));
    }

    protected void checkOutputs(FeatureCollection expectedFeatures, List<Output> userOutputs, List<Output> systemOutputs) throws IOException {
        Assertions.assertNotEquals(0, userOutputs.size());
        Assertions.assertNotEquals(0, systemOutputs.size());

        List<Feature>  exportedFeatures = new ArrayList<>();

        for (Output output : userOutputs) {
            if (output instanceof DownloadUrl downloadUrl)
                exportedFeatures.addAll(downloadFileAndSerializeFeatures(downloadUrl));
            //TODO: FeatureStatistics could get only checked if we also support during simulation "UPDATE_CALLBACK"
//            else if (output instanceof FeatureStatistics statistics)
//                Assertions.assertEquals(expectedFeatures.getFeatures().size(), statistics.getFeatureCount());
        }

        //TODO: FeatureStatistics could get only checked if we also support during simulation "UPDATE_CALLBACK"
//        for (Output output : systemOutputs) {
            //if we have one Feature - we expect at least one file
//            if (output instanceof FeatureStatistics statistics && expectedFeatures.getFeatures().size() > 1)
//                Assertions.assertTrue(statistics.getFileCount() > 0);
//        }

        List<String> existingFeaturesIdList = expectedFeatures.getFeatures().stream().map(Feature::getId).collect(Collectors.toList());
        List<String> exportedFeaturesFeaturesIdList = exportedFeatures.stream().map(Feature::getId).collect(Collectors.toList());

        Assertions.assertTrue(exportedFeaturesFeaturesIdList.containsAll(existingFeaturesIdList));
    }
}
