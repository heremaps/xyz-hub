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
package com.here.xyz.jobs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.datasets.files.Csv;
import com.here.xyz.jobs.datasets.files.FileInputSettings;
import com.here.xyz.jobs.datasets.files.GeoJson;
import com.here.xyz.jobs.steps.JobCompiler;
import com.here.xyz.jobs.steps.compiler.ImportFromFiles;
import com.here.xyz.jobs.steps.inputs.InputsFromS3;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.jobs.datasets.files.FileFormat.EntityPerLine;
import com.here.xyz.models.hub.Space;
import io.vertx.core.json.JsonObject;
import java.util.HashMap;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;


/**
 * TODO: Find issue why test are flickering on GitHub if we use three branches for index creations in
 * ImportFromFiles compiler. Till RC is not identified this test will be disabled
 */
@Disabled
public class ImportJobTestIT extends JobTest {
  private FeatureCollection setupFeatures;

  @BeforeEach
  public void setUp() {
    createSpace(new Space().withId(SPACE_ID).withVersionsToKeep(10000).withSearchableProperties(Map.of(
                    "foo1", true,
                    "foo2.nested", true,
                    "foo3.nested.array::array", true
            )
    ), false);
    setupFeatures = createKnownFeatures("setup", 10);
    putFeatureCollectionToSpace(SPACE_ID, setupFeatures);
  }

  @Test
  public void testInvalidFormat() throws Exception {
    Job importJob = buildImportJob(new Files<>().withInputSettings(
                    new FileInputSettings()
                            .withFormat(new Csv().withEntityPerLine(EntityPerLine.Feature))));
    Assertions.assertThrows(JobCompiler.CompilationError.class, () ->
            new ImportFromFiles()
                    .compile(importJob));
  }

  @Test
  public void testImportWithUserFiles() throws Exception {
    FeatureCollection importFeatures = createKnownFeatures("import", 5);

    Job importJob = buildImportJob(new Files<>().withInputSettings(
            new FileInputSettings()
                    .withFormat(new GeoJson().withEntityPerLine(EntityPerLine.Feature)))
    );
    createAndStartJob(importJob, toGeoJsonLines(importFeatures));

    checkImportedFeatures(importFeatures);
  }

  @Disabled("Just for local testing purpose")
  @Test
  public void testImportWithReferencedS3Files() throws Exception {
    FeatureCollection importFeatures = createKnownFeatures("import", 5);
    uploadFileToS3("s3_input_test/file.json", S3ContentType.APPLICATION_JSON,
            toGeoJsonLines(importFeatures), false);

    Job importJob = buildImportJob(new Files<>().withInputSettings(
            new FileInputSettings()
                    .withFormat(new GeoJson().withEntityPerLine(EntityPerLine.Feature)))
    );

    //The "inputs" property is WRITE_ONLY and therefore not part of the serialized Job. Send it as raw JSON payload instead.
    JsonObject jobPayload = new JsonObject(XyzSerializable.serialize(importJob))
            .put("inputs", new JsonObject()
                    .put("inputs", new JsonObject(XyzSerializable.serialize(
                            new InputsFromS3().withBucket("test-bucket").withPrefix("s3_input_test/")))));

    createJob(jobPayload);

    startJob(importJob.getId());
    pollJobStatus(importJob.getId());

    checkImportedFeatures(importFeatures);
  }


  @Test
  public void testImportWithReferencedS3Files2() throws Exception {
    FeatureCollection importFeatures = createKnownFeatures("import", 5);
    uploadFileToS3("s3_input_test/file.json", S3ContentType.APPLICATION_JSON,
            toGeoJsonLines(importFeatures), false);

    Job importJob = buildImportJob(
            new Files<>().withInputSettings(
            new FileInputSettings()
                    .withFormat(new GeoJson().withEntityPerLine(EntityPerLine.Feature)))
    );

    createJob(importJob);

    addS3InputToJob(importJob.getId(), "test-bucket", "s3_input_test/");

    startJob(importJob.getId());
    pollJobStatus(importJob.getId());

    checkImportedFeatures(importFeatures);
  }

  private void checkImportedFeatures(FeatureCollection importedFeatures) throws JsonProcessingException {
    Map<String, Feature> expected = new HashMap<>();
    setupFeatures.getFeatures().forEach(f -> expected.put(f.getId(), f));
    importedFeatures.getFeatures().forEach(f -> expected.put(f.getId(), f));

    Map<String, Feature> actual = getFeaturesFromSmallSpace(SPACE_ID, null, false)
            .getFeatures().stream().collect(Collectors.toMap(Feature::getId, f -> f));

    Assertions.assertEquals(expected.keySet(), actual.keySet());
    expected.forEach((id, exp) -> {
      Feature got = actual.get(id);
      Assertions.assertNotNull(got, "Missing feature " + id);
      //Skip the XyzNamespace which gets added during the write
      got.getProperties().setXyzNamespace(null);
      Assertions.assertEquals(XyzSerializable.serialize(exp.getGeometry()), XyzSerializable.serialize(got.getGeometry()));
      Assertions.assertEquals(XyzSerializable.serialize(exp.getProperties()), XyzSerializable.serialize(got.getProperties()));
    });
  }

  private Job buildImportJob(DatasetDescription source) {
    return new Job()
            .withId(JOB_ID)
            .withDescription("Import Job Test")
            .withSource(source)
            .withTarget(new DatasetDescription.Space<>().withId(SPACE_ID));
  }
}
