ARG ARCHITECTURE=arm64v8
FROM ${ARCHITECTURE}/eclipse-temurin:17

# Create our own user to avoid using root user directly
RUN useradd -rm -d /home/naksha -s /bin/bash -g root -G sudo -u 1001 naksha
USER naksha
WORKDIR /home/naksha

# Copy our artifacts into image
COPY build/libs/naksha-*-all.jar ./app/

# Docker Env variables with default values
ENV NAKSHA_CONFIG_ID test-config
ENV NAKSHA_ADMIN_DB_URL 'jdbc:postgresql://host.docker.internal:5432/postgres?user=postgres&password=password&schema=naksha&app=naksha_local&id=naksha_admin_db'

# Start application
CMD ["sh", "-c", "java -jar ./app/naksha-*-all.jar $NAKSHA_CONFIG_ID $NAKSHA_ADMIN_DB_URL"]
