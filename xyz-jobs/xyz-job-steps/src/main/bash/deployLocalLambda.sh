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

jarName=${2:-'xyz-job-steps-fat'}
handler=${3:-'com.here.xyz.jobs.steps.execution.LambdaBasedStep$LambdaBasedStepExecutor::handleRequest'}
relativeTargetPath="../../../target"

#TODO: Move the following into a lib.sh and include it from the other scripts
inContainer="$1"
if [ "$inContainer" = "true" ]; then
  LOCAL_STACK_HOST="http://host.docker.internal:4566"
  relativeTargetPath="."
  mkdir -p ~/.aws
  echo -e "[default]\nregion=us-east-1" > ~/.aws/config
  echo -e "[default]\naws_access_key_id = localstack\naws_secret_access_key = localstack" > ~/.aws/credentials
fi

#Create the local test bucket but only if not existing yet
aws --endpoint "$LOCAL_STACK_HOST" s3api head-bucket --bucket test-bucket > /dev/null 2>&1
if [ $? -ne 0 ]; then
  aws --endpoint "$LOCAL_STACK_HOST" s3api create-bucket --bucket test-bucket --create-bucket-configuration LocationConstraint=eu-west-1
fi

#Check if localstack is running
curl -s "$LOCAL_STACK_HOST/_localstack/health" > /dev/null 2>&1
if [ $? -ne 0 ]; then
  echo "local-stack container is not running properly!" >2
  exit 1
fi

#install zip
which zip > /dev/null 2>&1
if [ $? -ne 0 ]; then
  yum install -y zip
fi

#install Lambda
echo "Install "$jarName" Lambda ..."

scriptBasePath="$(dirname $(realpath $0))"

cd ${scriptBasePath}/${relativeTargetPath}

rm -rf lib > /dev/null 2>&1
mkdir lib
cp ./"$jarName".jar lib
zip -r "$jarName".zip lib
chmod -R 777 lib "$jarName".zip

#Delete a potentially existing old local Lambda Function with the same name
aws --endpoint "$LOCAL_STACK_HOST" lambda delete-function \
  --region us-east-1 \
  --function-name job-step \
  > /dev/null 2>&1

aws --endpoint "$LOCAL_STACK_HOST" lambda create-function \
  --timeout 300 \
  --region us-east-1 \
  --function-name job-step \
  --memory-size 512 \
  --runtime java17 \
  --zip-file fileb://"$jarName".zip \
  --handler "$handler" \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --environment "$(cat "$scriptBasePath/environment.json")"