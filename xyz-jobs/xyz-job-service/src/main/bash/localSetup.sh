#!/bin/bash

#
# Copyright (C) 2017-2024 HERE Europe B.V.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0
# License-Filename: LICENSE
#

state_machine_arn_prefix=arn:aws:states:us-east-1:000000000000:stateMachine:job-

aws --endpoint http://localhost:4566 events put-rule \
  --name StepFunctionStateChangeRule \
  --event-pattern "{\"source\":[\"aws.states\"],\"detail-type\":[\"Step Functions Execution Status Change\"],\"detail\":{\"stateMachineArn\":[{\"prefix\":\"$state_machine_arn_prefix\"}]}}" \
  --state ENABLED \
  --region us-east-1

connection_arn=$(aws --endpoint http://localhost:4566 events create-connection \
  --name JobApiConnection \
  --authorization-type API_KEY \
  --auth-parameters "ApiKeyAuthParameters={ApiKeyName=apiKey,ApiKeyValue=dummy-admin-api-key}" \
  --region us-east-1 | jq -r '.ConnectionArn')

api_destination_arn=$(aws --endpoint http://localhost:4566 events create-api-destination \
  --name JobApiDestination \
  --connection-arn "$connection_arn" \
  --invocation-endpoint http://host.docker.internal:7070/admin/state/events \
  --http-method POST \
  --region us-east-1 | jq -r '.ApiDestinationArn')


aws --endpoint http://localhost:4566 events put-targets \
  --rule StepFunctionStateChangeRule \
  --targets "Id"="JobApiDestination","Arn"="$api_destination_arn" \
  --region us-east-1
