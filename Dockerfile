FROM openjdk:17-slim

MAINTAINER Benjamin Rögner "benjamin.roegner@here.com"
MAINTAINER Lucas Ceni "lucas.ceni@here.com"
MAINTAINER Dimitar Goshev "dimitar.goshev@here.com"

ENV LOG_CONFIG log4j2-console-plain.json
ENV LOG_PATH /var/log/xyz
ENV FS_WEB_ROOT www

#Override the following environment variables to let the service connect to different host names
ENV STORAGE_DB_URL jdbc:postgresql://postgres/postgres
ENV PSQL_HOST postgres
ENV XYZ_HUB_REDIS_URI redis://redis
ENV PSQL_HTTP_CONNECTOR_HOST xyz-http-connector

COPY xyz-hub-service/target/xyz-hub-service.jar .
ADD Dockerfile /

EXPOSE 8080 9090
CMD java -jar xyz-hub-service.jar
