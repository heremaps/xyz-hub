ARG ARCHITECTURE=arm64v8
FROM ${ARCHITECTURE}/eclipse-temurin:17

# Copy license files
COPY LICENSE /
COPY HERE_NOTICE /

# Create our own user to avoid using root user directly
RUN useradd -rm -d /home/naksha -s /bin/bash -g root -G sudo -u 1001 naksha
USER naksha
WORKDIR /home/naksha

# Copy our artifacts into image
COPY --chown=naksha:root build/libs/naksha-*-all.jar ./app/
COPY --chown=naksha:root docker/cloud-config.json ./app/config/

# Copy the shell script & provide execute access
COPY --chown=naksha:root docker/run-app.sh ./app/
RUN chmod +x ./app/run-app.sh

# Docker Env variables with default values
ENV NAKSHA_CONFIG_ID test-config
ENV NAKSHA_ADMIN_DB_URL 'jdbc:postgresql://host.docker.internal:5432/postgres?user=postgres&password=password&schema=naksha&app=naksha_local&id=naksha_admin_db'
ENV NAKSHA_EXTENSION_S3_BUCKET 'naksha-pvt-releases'
ENV NAKSHA_JWT_PVT_KEY ''
ENV NAKSHA_JWT_PUB_KEY ''

# Execute Shell Script
ENTRYPOINT ["./app/run-app.sh"]
