#!/bin/bash

### Set env params based on App secrets fetched from AWS Secrets Manager

SECRET_ID="$EC2_ENV/heremap/service/naksha-v2"
SECRET_RESPONSE_JSON=`aws secretsmanager get-secret-value --region $EC2_REGION --secret-id $SECRET_ID`

# Validate that we really got the successful response
# This should match the original secret-id
RESPONSE_SECRET_ID=`echo $SECRET_RESPONSE_JSON | jq -r '.Name'`
if [[ "$SECRET_ID" != "$RESPONSE_SECRET_ID" ]]; then
  echo "ERROR :: Couldn't obtain Secrets for [$SECRET_ID]"
  exit 1
fi

# To convert JSON response secrets into environment variables
while read secret_key_value;
do
  #echo "export $secret_key_value"
  eval "export $secret_key_value"
done < <(echo $SECRET_RESPONSE_JSON | jq -r '.SecretString | fromjson | to_entries | map(@sh "\(.key)=\(.value)") | .[]')
