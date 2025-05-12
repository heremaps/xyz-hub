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

import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.datasets.files.FileInputSettings;
import com.here.xyz.jobs.datasets.files.GeoJson;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import com.here.xyz.jobs.util.test.ContentCreator;
import com.here.xyz.models.hub.Space;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.here.xyz.jobs.datasets.files.FileFormat.EntityPerLine.Feature;

/** TODO: Find issue why test are flickering on GitHub if we use three branches for index creations in
 * ImportFromFiles compiler. Till RC is not identified this test will be disabled */
@Disabled
public class ImportJobTestIT extends JobTest {
    @BeforeEach
    public void setUp() {
        createSpace(new Space().withId(SPACE_ID).withSearchableProperties(Map.of(
                "foo1.nested", true)), false);
    }

    @Test
    public void testSimpleImport() throws Exception {
        Job importJob = buildImportJob();
        createAndStartJob(importJob, ContentCreator.generateImportFileContent(ImportFilesToSpace.Format.GEOJSON, 50));
    }

    private Job buildImportJob() {
        return new Job()
                .withId(JOB_ID)
                .withDescription("Import Job Test")
                .withSource(new Files<>().withInputSettings(new FileInputSettings().withFormat(new GeoJson().withEntityPerLine(Feature))))
                .withTarget(new DatasetDescription.Space<>().withId(SPACE_ID));
    }
}
