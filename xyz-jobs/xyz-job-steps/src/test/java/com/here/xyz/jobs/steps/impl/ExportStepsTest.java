/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.jobs.steps.impl;

import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.jobs.util.S3Client;
import com.here.xyz.responses.StatisticsResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

import static com.here.xyz.jobs.datasets.space.UpdateStrategy.DEFAULT_UPDATE_STRATEGY;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType.START_EXECUTION;
import static java.lang.Thread.sleep;

public class ExportStepsTest extends JobStepsTest {

  @BeforeEach
  public void setUp() throws Exception {
    uploadInputFile(JOB_ID);
    LambdaBasedStep step = new ImportFilesToSpace()
            .withUpdateStrategy(DEFAULT_UPDATE_STRATEGY)
            .withSpaceId(SPACE_ID);

    sendLambdaStepRequest(step, START_EXECUTION);
    sleep(2000);
  }

  @Test
  public void testExportSpaceToFilesStep() throws Exception {
    StatisticsResponse statsBefore = getStatistics(SPACE_ID);

    LambdaBasedStep step = new ExportSpaceToFiles()
            .withSpaceId(SPACE_ID)
            .withFormat(ExportSpaceToFiles.Format.GEOJSON)
            .withJobId(JOB_ID);

    sendLambdaStepRequest(step, START_EXECUTION);
    sleep(2000);

    StatisticsResponse statsAfter = getStatistics(SPACE_ID);
    Assertions.assertEquals(statsBefore.getCount().getValue(), statsAfter.getCount().getValue());

    step.loadOutputs(true).forEach(
            output -> checkFile((DownloadUrl) output)
    );
  }

  private void checkFile(DownloadUrl output) {
      InputStream dataStream = S3Client.getInstance().streamObjectContent(((DownloadUrl) output).getS3Key());

      try {
        if (output.isCompressed())
          dataStream = new GZIPInputStream(dataStream);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(dataStream))) {
          String line;
          int lineCount = 0;
          while ((line = reader.readLine()) != null) {
              //@TODO: check content of lines (Features)
              lineCount++;
          }
          Assertions.assertEquals(2, lineCount);
        }
      }catch (IOException e) {
          throw new RuntimeException(e);
      }
  }

}
