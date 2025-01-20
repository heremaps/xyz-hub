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

inContainer=$1
jarName=$2
handler=$3

if [ "$inContainer" = "true" ]; then
  LOCAL_STACK_HOST="http://host.docker.internal:4566"
  mkdir -p ~/.aws
  echo -e "[default]\nregion=us-east-1" > ~/.aws/config
  echo -e "[default]\naws_access_key_id = localstack\naws_secret_access_key = localstack" > ~/.aws/credentials
fi

# Check if localstack is running
if aws --endpoint-url $LOCAL_STACK_HOST s3 ls "$LOCAL_GEOWARP_S3_URI" 2>&1 | grep -q 'Could not connect'; then
  echo "LocalStack is not running!"
  exit 1
elif aws --endpoint-url $LOCAL_STACK_HOST s3 ls "$LOCAL_GEOWARP_S3_URI" 2>&1 | grep -q 'NoSuchBucket'; then
  echo "Local Bucket "$LOCAL_BUCKET" is missing"
  exit 1
fi

#install zip
yum install -y zip

##############
# install Lambda
echo "INSTALL "$jarName" Lambda ....................."

rm -rf lib > /dev/null 2>&1
mkdir lib
cp ./"$jarName".jar lib
zip -r "$jarName".zip lib
chmod -R 777 lib "$jarName".zip

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
  --zip-file fileb://"$jarName".zip \
  --handler "$handler" \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --environment "$(cat "environment.json")" \
   > /dev/null 2>&1