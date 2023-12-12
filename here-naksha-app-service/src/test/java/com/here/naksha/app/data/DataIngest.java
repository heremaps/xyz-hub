/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.naksha.app.data;

import com.here.naksha.app.common.NakshaTestWebClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataIngest extends AbstractDataIngest {

  // Fixed variables
  private static final Logger logger = LoggerFactory.getLogger(DataIngest.class);

  // NOTE
  //  If you change the flag here, then ensure appropriate variables are set
  //  in respective ingestXXXData() functions
  private static final boolean TOPOLOGY_INGEST_ENABLED = false;
  private static final boolean TOPOLOGY_VIOLATION_INGEST_ENABLED = false;

  private static boolean topologyIngestEnabled() {
    return TOPOLOGY_INGEST_ENABLED;
  }

  private static boolean topologyViolationIngestEnabled() {
    return TOPOLOGY_VIOLATION_INGEST_ENABLED;
  }

  @Test
  @EnabledIf("topologyIngestEnabled")
  void ingestTopologyData() throws Exception {
    setNHUrl(nhUrl);
    setNHToken(nhToken);
    setNHSpaceId(nhSpaceId);
    setReqBatchSize(Integer.parseInt(DEF_BATCH_SIZE));
    setNakshaClient(new NakshaTestWebClient(nhUrl, 10, 90));

    logger.info("Ingesting Topology data using NH Url [{}], in Space [{}]", nhUrl, nhSpaceId);

    ingestData("topology/features.json");
  }

  @Test
  @EnabledIf("topologyViolationIngestEnabled")
  void ingestTopologyViolationData() throws Exception {
    setNHUrl(nhUrl);
    setNHToken(nhToken);
    setNHSpaceId(nhSpaceId);
    setReqBatchSize(Integer.parseInt(DEF_BATCH_SIZE));
    setNakshaClient(new NakshaTestWebClient(nhUrl, 10, 90));

    logger.info("Ingesting Topology Violation data using NH Url [{}], in Space [{}]", nhUrl, nhSpaceId);

    ingestData("topology/violations.json");
  }
}
