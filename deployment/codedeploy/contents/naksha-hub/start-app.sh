#!/bin/bash

# Set static configuration
export NAKSHA_CONFIG_PATH=$(pwd)/.config/
export CONFIG_ID=cloud-config
export LOG4J_CONFIGURATION_FILE=log4j2-rollingfile.xml

# Set instance specific parameters
. ./set-instance-params.sh

# Set application specific parameters
. ./set-app-params.sh

# Set Auth keys for application
. ./set-auth-keys.sh

if [ ! -d logs ]; then
  mkdir logs
fi

# Set local parameters
export OTEL_RESOURCE_ATTRIBUTES=service.name=${EC2_INSTANCE_NAME},service.namespace=Naksha-v2-${EC2_ENV_UPPER}

# Print basic parameters (avoid printing secrets)
echo "NAKSHA_CONFIG_PATH : $NAKSHA_CONFIG_PATH"
echo "CONFIG_ID : $CONFIG_ID"
echo "LOG4J_CONFIGURATION_FILE : $LOG4J_CONFIGURATION_FILE"
echo "OTEL_RESOURCE_ATTRIBUTES : $OTEL_RESOURCE_ATTRIBUTES"
echo "EC2_INSTANCE_NAME : $EC2_INSTANCE_NAME"
echo "EC2_ENV : $EC2_ENV"
echo "-Xms : $JVM_XMS"
echo "-Xmx : $JVM_XMX"

# Start service
java -javaagent:/home/admin/aws-opentelemetry/aws-opentelemetry-agent.jar \
  -server -Xms${JVM_XMS} -Xmx${JVM_XMX} -Xss1024k \
  -XX:+UnlockDiagnosticVMOptions \
  -XX:+UseZGC \
  -XX:+UseNUMA \
  -XX:+UseTLAB -XX:+AlwaysPreTouch \
  -XX:+ExplicitGCInvokesConcurrent \
  --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-opens java.base/java.lang=ALL-UNNAMED \
  -Xlog:gc*:file=logs/naksha_gc.log \
  -XX:ErrorFile=logs/naksha_hs_err_pid%p.log \
  -XX:LogFile=logs/naksha_hotspot.log \
  -jar naksha*.jar ${CONFIG_ID} ${NAKSHA_ADMIN_DB_URL} 1> logs/naksha_stdout.txt 2> logs/naksha_stderr.txt
