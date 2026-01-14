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

import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.datasets.files.Csv;
import com.here.xyz.jobs.datasets.files.FileInputSettings;
import com.here.xyz.jobs.datasets.files.GeoJson;
import com.here.xyz.jobs.steps.JobCompiler;
import com.here.xyz.jobs.steps.compiler.ImportFromFiles;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import com.here.xyz.jobs.util.test.ContentCreator;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Space;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.here.xyz.jobs.datasets.files.FileFormat.EntityPerLine.Feature;
import static com.here.xyz.jobs.datasets.files.FileFormat.EntityPerLine.FeatureCollection;

/**
 * TODO: Find issue why test are flickering on GitHub if we use three branches for index creations in
 * ImportFromFiles compiler. Till RC is not identified this test will be disabled
 */
@Disabled
public class ImportJobTestIT extends JobTest {
  @BeforeEach
  public void setUp() {
    createSpace(new Space().withId(SPACE_ID)
            .withVersionsToKeep(1000)
            .withSearchableProperties(Map.of(
                    "foo1", true,
                    "foo2.nested", true,
                    "foo3.nested.array::array", true
            )
    ), false);
  }

  @Test
  public void testSimpleImport() throws Exception {
    Job importJob = buildImportJob(new Files<>().withInputSettings(
            new FileInputSettings()
                    .withFormat(new GeoJson().withEntityPerLine(Feature))), SPACE_ID
    );
    createAndStartJob(importJob, ContentCreator.generateImportFileContent(ImportFilesToSpace.Format.GEOJSON, 50));
  }

  @Test
  public void testSimpleImportInComposite() throws Exception {
    String extensionSpaceId = SPACE_ID + "_ext";
    createSpace(new Space()
            .withId(extensionSpaceId)
            .withExtension(new Space.Extension().withSpaceId(SPACE_ID))
            .withVersionsToKeep(1000)
    , false);

    Job importJob = buildImportJob(new Files<>().withInputSettings(
            new FileInputSettings()
                    .withFormat(new GeoJson().withEntityPerLine(Feature))), SPACE_ID
    );

    Feature feature = XyzSerializable.deserialize("""
            {
                      "type": "Feature",
                      "id": "test",
                      "properties": {
                        "value" : "Germany"
                      },
                      "geometry": {
                        "coordinates": [
                          [
                            6.353809489694754,
                            51.08958733812065
                          ],
                          [
                            8.231999783047115,
                            51.359089906708846
                          ],
                          [
                            7.937709200611209,
                            50.5346535571293
                          ],
                          [
                            9.337868867777473,
                            50.26303968016663
                          ],
                          [
                            9.34797694869323,
                            49.3892593362186
                          ]
                        ],
                        "type": "LineString"
                      }
                    }
            """);
    createAndStartJob(importJob, feature.toByteArray());

    importJob = new Job()
            .withId(JOB_ID+"_ext")
            .withDescription("Import Job Test")
            .withSource(new Files<>().withInputSettings(
                            new FileInputSettings()
                                    .withFormat(new GeoJson().withEntityPerLine(Feature))))
            .withTarget(new DatasetDescription.Space<>().withId(extensionSpaceId));
    createAndStartJob(importJob, feature.toByteArray());
  }

  @Test
  public void testSimpleImportInNonEmpty() throws Exception {
    FeatureCollection fc = XyzSerializable.deserialize("""
                {
                  "type": "FeatureCollection",
                  "features": [
                    {
                      "type": "Feature",
                      "id": "line4_delta",
                      "properties": {
                        "value" : "Germany"
                      },
                      "geometry": {
                        "coordinates": [
                          [
                            6.353809489694754,
                            51.08958733812065
                          ],
                          [
                            8.231999783047115,
                            51.359089906708846
                          ],
                          [
                            7.937709200611209,
                            50.5346535571293
                          ],
                          [
                            9.337868867777473,
                            50.26303968016663
                          ],
                          [
                            9.34797694869323,
                            49.3892593362186
                          ]
                        ],
                        "type": "LineString"
                      }
                    }
                  ]
                }
                """, FeatureCollection.class);
    putFeatureCollectionToSpace(SPACE_ID, fc);
    Job importJob = buildImportJob(new Files<>().withInputSettings(
            new FileInputSettings()
                    .withFormat(new GeoJson().withEntityPerLine(Feature))), SPACE_ID
    );
    createAndStartJob(importJob, ContentCreator.generateImportFileContent(ImportFilesToSpace.Format.GEOJSON, 50));
  }

  @Test
  public void testInvalidEntity() throws Exception {
    Job importJob = buildImportJob(new Files<>().withInputSettings(
            new FileInputSettings()
                    .withFormat(new GeoJson().withEntityPerLine(FeatureCollection))), SPACE_ID
    );
    Assertions.assertThrows(JobCompiler.CompilationError.class, () ->
            new ImportFromFiles()
              .compile(importJob));
  }

  @Test
  public void testInvalidFormat() throws Exception {
    Job importJob = buildImportJob(new Files<>().withInputSettings(
                    new FileInputSettings()
                            .withFormat(new Csv().withEntityPerLine(Feature))), SPACE_ID);
    Assertions.assertThrows(JobCompiler.CompilationError.class, () ->
            new ImportFromFiles()
                    .compile(importJob));
  }

  private Job buildImportJob(DatasetDescription source, String targetSpaceId) {
    return new Job()
            .withId(JOB_ID)
            .withDescription("Import Job Test")
            .withSource(source)
            .withTarget(new DatasetDescription.Space<>().withId(targetSpaceId));
  }
}
