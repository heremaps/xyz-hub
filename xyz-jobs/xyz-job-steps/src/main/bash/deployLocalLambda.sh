#!/bin/bash

if [ "$(basename $(pwd))" != 'target' ]; then
  cd ../../../target
fi

rm job-step.jar
cp ./*.jar job-step.jar

#Delete a potentially existing old local Lambda Function with the same name
aws --endpoint http://localhost:4566 lambda delete-function \
  --region eu-west-1 \
  --function-name job-step \
  > /dev/null 2>&1

aws --endpoint http://localhost:4566 lambda create-function \
  --region eu-west-1 \
  --function-name job-step \
  --runtime java17 \
  --zip-file fileb://job-step.jar \
  --handler 'com.here.xyz.jobs.steps.execution.LambdaBasedStep$LambdaBasedStepExecutor::handleRequest' \
  --role arn:aws:iam::000000000000:role/lambda-role