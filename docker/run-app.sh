#!/bin/sh

# Set the NAKSHA_CONFIG_PATH
export NAKSHA_CONFIG_PATH=/home/naksha/app/config/

# Replace placeholder in cloud-config.json
sed -i "s+SOME_S3_BUCKET+${NAKSHA_EXTENSION_S3_BUCKET}+g" /home/naksha/app/config/cloud-config.json

# Check if NAKSHA_JWT_PVT_KEY is set and create jwt.key file if it is
if [ -n "$NAKSHA_JWT_PVT_KEY" ]; then
    mkdir -p ${NAKSHA_CONFIG_PATH}auth/
    echo "$NAKSHA_JWT_PVT_KEY" | sed 's/\\n/\n/g' > ${NAKSHA_CONFIG_PATH}auth/jwt.key
    echo "Using custom JWT private key"
else
    echo "No custom JWT private key supplied"
fi

# Check if NAKSHA_JWT_PUB_KEY is set and create jwt.pub file if it is
if [ -n "$NAKSHA_JWT_PUB_KEY" ]; then
    mkdir -p ${NAKSHA_CONFIG_PATH}auth/
    echo "$NAKSHA_JWT_PUB_KEY" | sed 's/\\n/\n/g' > ${NAKSHA_CONFIG_PATH}auth/jwt.pub
    echo "Using custom JWT public key"
else
    echo "No custom JWT public key supplied"
fi

# Start the application
java -jar /home/naksha/app/naksha-*-all.jar $NAKSHA_CONFIG_ID $NAKSHA_ADMIN_DB_URL