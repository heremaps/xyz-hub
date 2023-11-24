#!/bin/bash

### Set instance parameters based on EC2 instance tags

### Get Current Instance Id and region
export EC2_INSTANCE_ID=`curl -s http://169.254.169.254/latest/meta-data/instance-id`
export EC2_REGION=`curl -s http://169.254.169.254/latest/meta-data/placement/region`

if [[ "$EC2_INSTANCE_ID" == "" ]] || [[ "$EC2_REGION" == "" ]]; then
  echo "ERROR :: Couldn't obtain EC2 instance metadata information"
  exit 1
fi


### Retrieve necessary tags for current instance Id
TAGS_RESPONSE_JSON=`aws ec2 describe-tags --filters "Name=resource-id,Values=$EC2_INSTANCE_ID" --region=$EC2_REGION`
# read value for "Environment" tag (i.e. dev, e2e, prd)
export EC2_ENV=`echo $TAGS_RESPONSE_JSON | jq -r '.Tags | .[] | select(.Key == "Environment") | .Value'`
export EC2_INSTANCE_NAME=`echo $TAGS_RESPONSE_JSON | jq -r '.Tags | .[] | select(.Key == "Name") | .Value'`

if [[ "$EC2_ENV" == "" ]] || [[ "$EC2_INSTANCE_NAME" == "" ]]; then
  echo "ERROR :: Couldn't find one or more of mandatory EC2 tags : [Environment, Name]"
  exit 1
fi

# Convert "dev" to upper case "DEV" :
#     echo $environment | tr '[:lower:]' '[:upper:]'
export EC2_ENV_UPPER=`echo $EC2_ENV | tr '[:lower:]' '[:upper:]'`
