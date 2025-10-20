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

package com.here.xyz.jobs.steps.impl.transport;

import com.here.xyz.jobs.steps.S3DataFile;
import com.here.xyz.jobs.steps.execution.StepException;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.inputs.UploadUrl;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.steps.resources.TooManyResourcesClaimed;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.psql.query.QueryBuilder;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import io.vertx.core.json.JsonObject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.ExecutionMode.ASYNC;
import static com.here.xyz.util.web.XyzWebClient.WebClientException;


/**
 * This step imports a set of user provided inputs and imports their data into a specified space. This step produces exactly one output of
 * type {@link FeatureStatistics}.
 */
public class ImportFilesToSpaceV2 extends TaskedSpaceBasedStep<ImportFilesToSpaceV2> {

  public ImportFilesToSpaceV2 withVersionRef(Ref versionRef) {
    setVersionRef(versionRef);
    return this;
  }

  @Override
  protected int setInitialThreadCount(String schema) throws WebClientException, SQLException, TooManyResourcesClaimed {
    return 15;
  }

  @Override
  protected List<TaskData> createTaskItems(String schema) throws WebClientException, SQLException, TooManyResourcesClaimed, QueryBuilder.QueryBuildingException {
    List<?> inputs = loadInputs(UploadUrl.class);
    if (inputs.isEmpty()) {
      throw new StepException("No valid inputs of type 'UploadUrl' found.");
    }
    List<TaskData> taskItems = new ArrayList<>();
    for (Input input : (List<Input>) inputs) {
      if (input instanceof S3DataFile) {
        taskItems.add(new TaskData(
                new JsonObject()
                        .put("s3Bucket", input.getS3Bucket())
                        .put("s3Key", input.getS3Key())
                        .put("byteSize", input.getByteSize())

        ));
      }
    }

    return taskItems;
  }

  @Override
  protected SQLQuery buildTaskQuery(String schema, Integer taskId, TaskData taskData) throws QueryBuilder.QueryBuildingException, TooManyResourcesClaimed, WebClientException, InvalidGeometryException {
    return null;
  }

  @Override
  public List<Load> getNeededResources() {
    return List.of();
  }

  @Override
  public int getTimeoutSeconds() {
    return 3 * 24 * 3600;
  }

  @Override
  public String getDescription() {
    return "ImportsV2 the data to space " + getSpaceId();
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    //TBD
    return 100;
  }

  @Override
  public ExecutionMode getExecutionMode() {
    return ASYNC;
  }

  @Override
  public boolean validate() throws ValidationException {
    super.validate();
    //TBD
//    try {
//      infoLog(JOB_VALIDATE, this);
//      //Check if the space is actually existing
//      Space space = space();
//      if (!space.isActive())
//        throw new ValidationException("Data can not be written to target " + space.getId() + " as it is inactive.");
//
//      if (space.isReadOnly())
//        throw new ValidationException("Data can not be written to target " + space.getId() + " as it is in read-only mode.");
//
//      if (entityPerLine == FeatureCollection && format == CSV_JSON_WKB)
//        throw new ValidationException("Combination of entityPerLine 'FeatureCollection' and type 'Csv' is not supported!");
//
//      if (loadTargetSpaceFeatureCount() > 0 && getUncompressedUploadBytesEstimation() > getMaxInputBytesForNonEmptyImport())
//        throw new ValidationException("An import into a non empty space is not possible. "
//                + "The uncompressed size of the provided files exceeds the limit of " + getMaxInputBytesForNonEmptyImport() + " bytes.");
//    }
//    catch (WebClientException e) {
//      throw new ValidationException("Error loading resource " + getSpaceId(), e);
//    }
//
//    if (isUserInputsExpected()) {
//      if (!isUserInputsPresent(UploadUrl.class))
//        return false;
//      //Quick-validate the first UploadUrl that is found in the inputs
//      if(enableQuickValidation)
//        ImportFilesQuickValidator.validate(loadInputsSample(1, UploadUrl.class).get(0), format, entityPerLine);
//    }

    return true;
  }

  private SQLQuery buildImportFromS3PluginQuery(String schema, int taskId, String contentQuery) throws WebClientException {

    return new SQLQuery(
            "SELECT export_to_s3_perform(#{taskId},  #{s3_bucket}, #{s3_path}, #{s3_region}, #{step_payload}::JSON->'step', " +
                    "#{lambda_function_arn}, #{lambda_region}, #{contentQuery}, '${{failureCallback}}');")
            .withContext(getQueryContext(schema))
            .withAsyncProcedure(false)
            .withNamedParameter("taskId", taskId)
            //.withNamedParameter("s3_bucket", downloadUrl.getS3Bucket())
            //.withNamedParameter("s3_path", downloadUrl.getS3Key())
            .withNamedParameter("s3_region", bucketRegion())
            .withNamedParameter("step_payload", new LambdaStepRequest().withStep(this).serialize())
            .withNamedParameter("lambda_function_arn", getwOwnLambdaArn().toString())
            .withNamedParameter("lambda_region", getwOwnLambdaArn().getRegion())
            .withNamedParameter("contentQuery", contentQuery)
            .withQueryFragment("failureCallback",  buildFailureCallbackQuery().substitute().text().replaceAll("'", "''"));
  }

}
