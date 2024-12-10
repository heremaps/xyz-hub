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

check_localstack_health() {
    local RETRIES=10
    local RETRY_DELAY=5
    local SERVICE="healthy"

    for ((i=0; i < RETRIES; i++)); do
        if awslocal s3api list-buckets >/dev/null 2>&1; then
            echo "LocalStack S3 service is ready."
            return 0
        else
            echo "Waiting for LocalStack S3 service to be ready... (attempt $((i+1))/${RETRIES})"
            sleep $RETRY_DELAY
        fi
    done

    echo "LocalStack S3 service is not ready after ${RETRIES} attempts. Exiting..."
    exit 1
}

check_localstack_health

awslocal s3api create-bucket --bucket test-bucket --create-bucket-configuration LocationConstraint=eu-west-1