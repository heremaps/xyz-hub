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

public class Config extends BaseConfig {
  public static Config instance;
  /**
   * The arn of the secret (in Secret Manager) that contains bot credentials.
   */
  public String JOB_BOT_SECRET_ARN;
  /**
   * Hub-Endpoint
   */
  public String HUB_ENDPOINT;

  {
    instance = this;
  }

  /**
   * ECPS_PHRASE of Default Connector
   */
  public String ECPS_PHRASE;
  /**
   * S3 Bucket for imports/exports
   */
  public String JOBS_S3_BUCKET;
  /**
   * Region in which components are running/hosted
   */
  public String JOBS_REGION;
  /**
   * S3/CW/Dynamodb localstack endpoints
   */
  public URI LOCALSTACK_ENDPOINT;

}
