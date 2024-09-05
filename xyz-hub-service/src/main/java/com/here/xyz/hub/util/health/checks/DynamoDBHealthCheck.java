/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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
package com.here.xyz.hub.util.health.checks;

import static com.here.xyz.hub.util.health.schema.Status.Result.CRITICAL;
import static com.here.xyz.hub.util.health.schema.Status.Result.OK;

import com.amazonaws.services.dynamodbv2.model.ExecuteStatementRequest;
import com.here.xyz.hub.util.health.schema.Response;
import com.here.xyz.hub.util.health.schema.Status;
import com.here.xyz.util.ARN;
import com.here.xyz.util.service.aws.dynamo.DynamoClient;
import java.net.URI;
import java.net.URISyntaxException;

public class DynamoDBHealthCheck extends DBHealthCheck {

	private final DynamoClient dynamoClient;

	public DynamoDBHealthCheck(ARN tableArn) {
		super(toURI(tableArn));
		setName("DynamoDB");
		dynamoClient = new DynamoClient(tableArn.toString(), null);
	}

	private static URI toURI(ARN arn) {
    try {
      return new URI(arn.toString());
    }
    catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

	@Override
	public Status execute() {
		Status s = new Status();
		Response r = new Response();
		try {
			dynamoClient.executeStatementSync(
					new ExecuteStatementRequest().withStatement("SELECT id FROM \"" + dynamoClient.tableName + "\" WHERE \"id\" = 'healthCheckTestCall'"));
			setResponse(null);
			return s.withResult(OK);
		}
		catch (Exception e) {
			setResponse(r.withMessage("Error when trying to connect to Dynamo: " + e.getMessage()));
			return s.withResult(CRITICAL);
		}
	}
}
