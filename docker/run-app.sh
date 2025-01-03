#!/bin/sh

# Set the NAKSHA_CONFIG_PATH
export NAKSHA_CONFIG_PATH=/home/naksha/app/config/

NAKSHA_PVT_KEY_PATH=""
NAKSHA_PUB_KEY_PATHS=""

# Check if NAKSHA_JWT_PVT_KEY is set and create jwt.key file if it is
if [ -n "$NAKSHA_JWT_PVT_KEY" ]; then
    mkdir -p ${NAKSHA_CONFIG_PATH}auth/
    KEY_FILE_PATH=auth/jwt.key
    echo "$NAKSHA_JWT_PVT_KEY" | sed 's/\\n/\n/g' > ${NAKSHA_CONFIG_PATH}${KEY_FILE_PATH}
    NAKSHA_PVT_KEY_PATH=${KEY_FILE_PATH}
    echo "Using custom JWT private key"
else
    echo "No custom JWT private key supplied"
fi

# Check if NAKSHA_JWT_PUB_KEY is set and create jwt.pub file if it is
if [ -n "$NAKSHA_JWT_PUB_KEY" ]; then
    mkdir -p ${NAKSHA_CONFIG_PATH}auth/
    KEY_FILE_PATH=auth/jwt.pub
    echo "$NAKSHA_JWT_PUB_KEY" | sed 's/\\n/\n/g' > ${NAKSHA_CONFIG_PATH}${KEY_FILE_PATH}
    NAKSHA_PUB_KEY_PATHS=${KEY_FILE_PATH}
    echo "Using custom JWT public key"
else
    echo "No custom JWT public key supplied"
fi

# Check if NAKSHA_JWT_PUB_KEY_2 is set and create jwt_2.pub file if it is
if [ -n "$NAKSHA_JWT_PUB_KEY_2" ]; then
    mkdir -p ${NAKSHA_CONFIG_PATH}auth/
    KEY_FILE_PATH=auth/jwt_2.pub
    echo "$NAKSHA_JWT_PUB_KEY_2" | sed 's/\\n/\n/g' > ${NAKSHA_CONFIG_PATH}${KEY_FILE_PATH}
    NAKSHA_PUB_KEY_PATHS=${NAKSHA_PUB_KEY_PATHS},${KEY_FILE_PATH}
    echo "Using custom JWT public key 2"
else
    echo "No custom JWT public key 2 supplied"
fi

# Replace all placeholders in cloud-config.json
sed -i "s+SOME_S3_BUCKET+${NAKSHA_EXTENSION_S3_BUCKET}+g" /home/naksha/app/config/cloud-config.json
sed -i "s+SOME_PVT_KEY_PATH+${NAKSHA_PVT_KEY_PATH}+g" /home/naksha/app/config/cloud-config.json
sed -i "s+SOME_PUB_KEY_PATHS+${NAKSHA_PUB_KEY_PATHS}+g" /home/naksha/app/config/cloud-config.json

# Start the application
java $JAVA_OPTS -jar /home/naksha/app/naksha-*-all.jar $NAKSHA_CONFIG_ID $NAKSHA_ADMIN_DB_URL