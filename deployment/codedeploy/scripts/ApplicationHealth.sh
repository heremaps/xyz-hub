#!/bin/bash

#### NOTE :
#### This file is not used at present, as during deployment, ELB itself takes care of checking application health before opening up the traffic

#### Wait for application to be healthy

# set internal parameters
APP_PORT=7080
MAX_TIMEOUT_SEC=120
SLEEP_INTERVAL_SEC=2

# run in a loop waiting for success
START_TIME=`date +%s`
WAIT_TIME=0

while [ 1 ];
do
  # Fetch application health status
  STATUS=`curl -s localhost:$APP_PORT | jq -r '.status'`

  if [[ "$STATUS" == "OK" ]];
  then
    echo "Application is up and running! ... after [$WAIT_TIME] seconds"
    exit 0
  fi

  # Exit if we have exceeded max wait time
  CRT_TIME=`date +%s`
  let "WAIT_TIME=$CRT_TIME - $START_TIME"

  if [[ $WAIT_TIME -gt $MAX_TIMEOUT_SEC ]];
  then
    echo "Error:: Exceeded timeout [$MAX_TIMEOUT_SEC] sec, while waiting for application to be healthy."
    exit 1
  fi

  echo "Waited [$WAIT_TIME] seconds..."
  # Sleep and try checking status again
  sleep $SLEEP_INTERVAL_SEC
done

exit 1
