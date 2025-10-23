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
set -e

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

# Create or get the event rule
rule_arn=$(aws --endpoint "$LOCAL_STACK_HOST" events describe-rule --name StepFunctionStateChangeRule --region us-east-1 \
  --query 'Arn' --output text 2>/dev/null || \
  aws --endpoint "$LOCAL_STACK_HOST" events put-rule \
    --name StepFunctionStateChangeRule \
    --event-pattern "{\"source\":[\"aws.states\"],\"detail-type\":[\"Step Functions Execution Status Change\"],\"detail\":{\"stateMachineArn\":[{\"prefix\":\"$state_machine_arn_prefix\"}]}}" \
    --state ENABLED \
    --region us-east-1 \
    --query 'RuleArn' --output text)

# Create or get the connection
connection_arn=$(aws --endpoint "$LOCAL_STACK_HOST" events list-connections --region us-east-1 \
  --query "Connections[?Name=='JobApiConnection'].ConnectionArn" --output text)
if [ -z "$connection_arn" ]; then
  connection_arn=$(aws --endpoint "$LOCAL_STACK_HOST" events create-connection \
    --name JobApiConnection \
    --authorization-type API_KEY \
    --auth-parameters "ApiKeyAuthParameters={ApiKeyName=apiKey,ApiKeyValue=dummy-admin-api-key}" \
    --region us-east-1 \
    --query 'ConnectionArn' --output text)
fi

# Create or get the API destination
api_destination_arn=$(aws --endpoint "$LOCAL_STACK_HOST" events list-api-destinations --region us-east-1 \
  --query "ApiDestinations[?Name=='JobApiDestination'].ApiDestinationArn" --output text)
if [ -z "$api_destination_arn" ]; then
  api_destination_arn=$(aws --endpoint "$LOCAL_STACK_HOST" events create-api-destination \
    --name JobApiDestination \
    --connection-arn "$connection_arn" \
    --invocation-endpoint http://host.docker.internal:7070/admin/state/events \
    --http-method POST \
    --region us-east-1 \
    --query 'ApiDestinationArn' --output text)
fi

# Put targets (idempotent)
aws --endpoint "$LOCAL_STACK_HOST" events put-targets \
  --rule StepFunctionStateChangeRule \
  --targets "Id"="JobApiDestination","Arn"="$api_destination_arn" \
  --region us-east-1

echo "Job service setup completed."