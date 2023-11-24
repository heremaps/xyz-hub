#!/bin/bash

## Argument 1 = CodeDeploy deploymentId, which is to be monitored for success
## Argument 2 = Max Timeout in seconds, for which this script should monitor the status before failing the deployment

# Check if required parameters are provided
if [[ "$1" == "" ]] || [[ "$2" == "" ]]; then
  echo "Error:: Missing mandatory parameters - deploymentId and maxTimeOut"
  exit -1
fi

# set internal parameters
DEPLOYMENT_ID=$1
MAX_TIMEOUT_SEC=$2
SLEEP_INTERVAL_SEC=5

# start script from here
echo "Monitoring status of deployment Id [$DEPLOYMENT_ID], with max timeout of [$MAX_TIMEOUT_SEC] seconds"

START_TIME=`date +%s`

# Wait for final status of deployment
while [ 1 ];
do
  # Fetch deployment details
  DEPLOYMENT_RESPONSE_JSON=`aws deploy get-deployment --deployment-id $DEPLOYMENT_ID`

  # Extract deployment details from JSON response
  STATUS=`echo $DEPLOYMENT_RESPONSE_JSON | jq -r '.deploymentInfo.status'`
  PENDING_CNT=`echo $DEPLOYMENT_RESPONSE_JSON | jq '.deploymentInfo.deploymentOverview.Pending'`
  IN_PROGRESS_CNT=`echo $DEPLOYMENT_RESPONSE_JSON | jq '.deploymentInfo.deploymentOverview.InProgress'`
  SUCCESS_CNT=`echo $DEPLOYMENT_RESPONSE_JSON | jq '.deploymentInfo.deploymentOverview.Succeeded'`
  FAIL_CNT=`echo $DEPLOYMENT_RESPONSE_JSON | jq '.deploymentInfo.deploymentOverview.Failed'`
  SKIP_CNT=`echo $DEPLOYMENT_RESPONSE_JSON | jq '.deploymentInfo.deploymentOverview.Skipped'`

  # exit if failed or stopped
  if [[ "$STATUS" == "Failed" ]] || [[ "$STATUS" == "Stopped" ]];
  then
    echo "Error:: Deployment failed/stopped. Overall status is: "
    echo $DEPLOYMENT_RESPONSE_JSON | jq
    exit 1
  fi

  # Even if Status is successful, we still need to check count of successful EC2 instances
  if [[ "$STATUS" == "Succeeded" ]];
  then
    let "TOTAL_PENDING_COUNT=$PENDING_CNT + $IN_PROGRESS_CNT"
    let "TOTAL_UNSUCCESSFUL_COUNT=$FAIL_CNT + $SKIP_CNT"

    if [[ $TOTAL_PENDING_COUNT -gt 0 ]];
    then
      # One or more EC2 instance(s) yet to be updated. Keep waiting.
      null;
    elif [[ $TOTAL_UNSUCCESSFUL_COUNT -gt 0 ]];
    then
      echo "Error:: Deployment failed. Overall status is: "
      echo $DEPLOYMENT_RESPONSE_JSON | jq
      exit 1
    fi

    echo "Deployment successfully completed for $SUCCESS_CNT instances!"
    exit 0
  fi

  # Exit if we have exceeded max wait time
  CRT_TIME=`date +%s`
  let "WAIT_TIME=$CRT_TIME - $START_TIME"

  # Print current status
  echo "Current status is [ status : $STATUS, pending : $PENDING_CNT, in_progress : $IN_PROGRESS_CNT, success : $SUCCESS_CNT, failed : $FAIL_CNT, skipped : $SKIP_CNT ]. Waited [$WAIT_TIME] seconds ..."

  if [[ $WAIT_TIME -gt $MAX_TIMEOUT_SEC ]];
  then
    echo "Error:: Exceeded timeout [$MAX_TIMEOUT_SEC] sec, while waiting for deployment [$DEPLOYMENT_ID] to complete."
    exit 1
  fi

  # Sleep and try checking status again
  sleep $SLEEP_INTERVAL_SEC
done

exit 1