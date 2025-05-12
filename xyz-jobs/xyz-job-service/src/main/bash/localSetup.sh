#!/bin/bash

#
# Copyright (C) 2017-2025 HERE Europe B.V.
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

LOCAL_STACK_HOST="http://localhost:4566"

#TODO: Move the following into a lib.sh and include it from the other scripts
inContainer=$1
if [ "$inContainer" = "true" ]; then
  LOCAL_STACK_HOST="http://host.docker.internal:4566"
  mkdir -p ~/.aws
  echo -e "[default]\nregion=us-east-1" > ~/.aws/config
  echo -e "[default]\naws_access_key_id = localstack\naws_secret_access_key = localstack" > ~/.aws/credentials
fi

#Create the local test bucket but only if not existing yet
aws --endpoint "$LOCAL_STACK_HOST" s3api head-bucket --bucket test-bucket > /dev/null 2>&1
if [ $? -ne 0 ]; then
  aws --endpoint "$LOCAL_STACK_HOST" s3api create-bucket --bucket test-bucket --create-bucket-configuration LocationConstraint=eu-west-1
fi

#Create the local event rule / heartbeat trigger
state_machine_arn_prefix=arn:aws:states:us-east-1:000000000000:stateMachine:job-

aws --endpoint "$LOCAL_STACK_HOST" events put-rule \
  --name StepFunctionStateChangeRule \
  --event-pattern "{\"source\":[\"aws.states\"],\"detail-type\":[\"Step Functions Execution Status Change\"],\"detail\":{\"stateMachineArn\":[{\"prefix\":\"$state_machine_arn_prefix\"}]}}" \
  --state ENABLED \
  --region us-east-1

connection_arn=$(aws --endpoint "$LOCAL_STACK_HOST" events create-connection \
  --name JobApiConnection \
  --authorization-type API_KEY \
  --auth-parameters "ApiKeyAuthParameters={ApiKeyName=apiKey,ApiKeyValue=dummy-admin-api-key}" \
  --region us-east-1 | sed -n 's/.*"ConnectionArn":\s*"\([^"]*\)".*/\1/p')

#Create the local event bridge rule to forward SFN events to the Job Service
api_destination_arn=$(aws --endpoint "$LOCAL_STACK_HOST" events create-api-destination \
  --name JobApiDestination \
  --connection-arn "$connection_arn" \
  --invocation-endpoint http://host.docker.internal:7070/admin/state/events \
  --http-method POST \
  --region us-east-1 | sed -n 's/.*"ApiDestinationArn":\s*"\([^"]*\)".*/\1/p')

aws --endpoint "$LOCAL_STACK_HOST" events put-targets \
  --rule StepFunctionStateChangeRule \
  --targets "Id"="JobApiDestination","Arn"="$api_destination_arn" \
  --region us-east-1

echo "Job service setup completed."