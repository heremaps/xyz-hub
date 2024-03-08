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

package com.here.xyz.httpconnector.job.resources;

import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.DBCluster;
import com.amazonaws.services.rds.model.DescribeDBClustersRequest;
import com.amazonaws.services.rds.model.DescribeDBClustersResult;

public class AwsRDSClient {

  private static final AwsRDSClient instance = new AwsRDSClient();
  private final AmazonRDS client;

  private AwsRDSClient() {
    AmazonRDSClientBuilder builder = AmazonRDSClientBuilder.standard();

    this.client = builder.build();
  }

  public static AwsRDSClient getInstance() {
    return instance;
  }

  public DBCluster getRDSClusterConfig(String clusterId) {
    DescribeDBClustersResult result = client.describeDBClusters(new DescribeDBClustersRequest()
        .withDBClusterIdentifier(clusterId));
    return result.getDBClusters().size() == 0 ? null : result.getDBClusters().get(0);
  }

}
