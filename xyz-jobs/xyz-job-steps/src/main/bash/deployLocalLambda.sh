#!/bin/bash

if [ "$(basename $(pwd))" != 'target' ]; then
  cd ../../../target
fi

rm -rf lib > /dev/null 2>&1
mkdir lib
cp ./xyz-job-steps-fat.jar lib
zip -r xyz-job-steps.zip lib

#Delete a potentially existing old local Lambda Function with the same name
aws --endpoint http://localhost:4566 lambda delete-function \
  --region us-east-1 \
  --function-name job-step \
  > /dev/null 2>&1

aws --endpoint http://localhost:4566 lambda create-function \
  --timeout 30 \
  --region us-east-1 \
  --function-name job-step \
  --runtime java17 \
  --zip-file fileb://xyz-job-steps.zip \
  --handler 'com.here.xyz.jobs.steps.execution.LambdaBasedStep$LambdaBasedStepExecutor::handleRequest' \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --environment 'Variables={HUB_ENDPOINT=http://host.docker.internal:8080/hub,ECPS_PHRASE=local,JOBS_S3_BUCKET=test-bucket,JOBS_REGION=us-east-1,LOCALSTACK_ENDPOINT=http://localstack:4566,LOCAL_DB_HOST_OVERRIDE=postgres}'