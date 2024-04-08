ARG ARCHITECTURE=arm64v8
FROM ${ARCHITECTURE}/eclipse-temurin:17

RUN mkdir /opt/app
COPY build/libs/naksha-*-all.jar /opt/app

ENV NAKSHA_CONFIG_ID test-config
ENV NAKSHA_ADMIN_DB_URL 'jdbc:postgresql://host.docker.internal:5432/postgres?user=postgres&password=password&schema=naksha&app=naksha_local&id=naksha_admin_db'

CMD ["sh", "-c", "java -jar /opt/app/naksha-*-all.jar $NAKSHA_CONFIG_ID $NAKSHA_ADMIN_DB_URL"]
