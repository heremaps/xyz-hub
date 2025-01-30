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

LOCAL_STACK_HOST="http://localhost:4566"

relativeTargetPath="../../../target"

if [ "$HOSTNAME" = "localstack" ]; then
  relativeTargetPath="../job-steps/target"
fi

#Check if the localstack is up and running
curl -sf $LOCAL_STACK_HOST/_localstack/health > /dev/null
if [ $? -ne 0 ]; then
  echo "Localstack is not up and running." >&2
  exit 1
fi

scriptBasePath="$(dirname $(realpath $0))"

cd ${scriptBasePath}/${relativeTargetPath}

rm -rf lib > /dev/null 2>&1
mkdir lib
cp ./xyz-job-steps-fat.jar lib
zip -r xyz-job-steps.zip lib
chmod -R 777 lib xyz-job-steps.zip

#Delete a potentially existing old local Lambda Function with the same name
aws --endpoint $LOCAL_STACK_HOST lambda delete-function \
  --region us-east-1 \
  --function-name job-step \
  > /dev/null 2>&1

aws --endpoint $LOCAL_STACK_HOST lambda create-function \
  --timeout 300 \
  --region us-east-1 \
  --function-name job-step \
  --memory-size 512 \
  --runtime java17 \
  --zip-file fileb://xyz-job-steps.zip \
  --handler 'com.here.xyz.jobs.steps.execution.LambdaBasedStep$LambdaBasedStepExecutor::handleRequest' \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --environment "$(cat "$scriptBasePath/environment.json")"