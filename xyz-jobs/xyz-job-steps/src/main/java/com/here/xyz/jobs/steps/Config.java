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

package com.here.xyz.jobs.steps;

import com.here.xyz.util.service.BaseConfig;
import java.net.URI;
import java.net.URL;
import java.util.List;

public class Config extends BaseConfig {
  public static Config instance;

  {
    instance = this;
  }

  /**
   * The arn of the secret (in Secret Manager) that contains AWS bot credentials.
   */
  public String JOB_BOT_SECRET_ARN;

  /**
   * Hub-Endpoint
   */
  public String HUB_ENDPOINT;

  /**
   * ECPS_PHRASE of Default Connector
   */
  public String ECPS_PHRASE;

  /**
   * The S3 Bucket for all inputs / outputs of the job framework
   */
  public String JOBS_S3_BUCKET;

  /**
   * The localstack endpoint to use it for all AWS clients when running locally
   *
   * NOTE: This config variable may only be set when running locally, because the system uses it as an indicator
   *  to determine whether it's running locally or on AWS.
   */
  public URI LOCALSTACK_ENDPOINT;

  /**
   * The DB hostname to be used inside the step lambda when running locally
   */
  public String LOCAL_DB_HOST_OVERRIDE;

  /**
   * The load balancer endpoint of the job API, to be used by other components to call the job API (admin-)endpoints.
   */
  public URL JOB_API_ENDPOINT;

  /**
   * A string that contains the full qualified class names of Job plugins, separated by comma
   */
  public String STEP_PLUGINS;

  public List<String> stepPlugins() {
    return fromCommaSeparatedList(STEP_PLUGINS);
  }

  /**
   * A comma separated list of AWS regions from which to not allow reading data (e.g. for import)
   */
  public String FORBIDDEN_SOURCE_REGIONS;

  public List<String> forbiddenSourceRegions() {
    return fromCommaSeparatedList(FORBIDDEN_SOURCE_REGIONS);
  }
}
