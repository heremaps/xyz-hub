#!/bin/bash

while getopts "c:r:l:t:" flag
do
     case "${flag}" in
	    c) CONCURRENCY=${OPTARG};;
	    r) RAMP_UP_OPT=${OPTARG};;
	    l) LOOP_COUNT_OPT=${OPTARG};;
	    t) TIMEOUT=${OPTARG};;
     esac
done

if [ -z "$CONCURRENCY" ] || [ -z "$TIMEOUT" ]; then
	echo 'Missing -c (for CONCURRENCY) or -t (for REQUEST TIMEOUT)' >&2
        exit 1
fi

if [[ -z "$RAMP_UP_OPT" ]]; then
    RAMP_UP=2
else
    RAMP_UP=$RAMP_UP_OPT
fi

if [[ -z "$LOOP_COUNT_OPT" ]]; then
    LOOP_COUNT=20
else
    LOOP_COUNT=$LOOP_COUNT_OPT
fi

DATE=$(date +%Y_%m_%d_%H:%M:%S)
DIR="c=$CONCURRENCY-t=$TIMEOUT-r=$RAMP_UP-l=$LOOP_COUNT-$DATE"
mkdir $DIR

jmeter -n \
 -t naksha_local_load.jmx \
 -Jthreads=$CONCURRENCY \
 -JrampUp=$RAMP_UP \
 -JloopCount=$LOOP_COUNT \
 -l $DIR/results.csv >> $DIR/output

SCRIPT_DURATION=${SECONDS}s
echo "JMeter test took $SCRIPT_DURATION, saving outcome and container logs in $DIR"
podman logs --since $SCRIPT_DURATION naksha-app &> $DIR/naksha-app.logs
podman logs --since $SCRIPT_DURATION naksha-psql &> $DIR/naksha-psql.logs
