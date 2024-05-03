#!/bin/bash

### Create Auth key files based on App secrets fetched from AWS Secrets Manager

# Note : -en flag converts '\n' to a new line, while writing into a file)
# Set private key file
echo -en $JWT_PVT_KEY > ${NAKSHA_CONFIG_PATH}auth/jwt.key
# Set public key file
echo -en $JWT_PUB_KEY > ${NAKSHA_CONFIG_PATH}auth/jwt.pub
