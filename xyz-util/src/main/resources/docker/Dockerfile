FROM openjdk:17-slim

MAINTAINER Benjamin Rögner "benjamin.roegner@here.com"
MAINTAINER Lucas Ceni "lucas.ceni@here.com"
MAINTAINER Dimitar Goshev "dimitar.goshev@here.com"

ENV LOG_CONFIG log4j2-console-plain.json
ENV LOG_PATH /var/log/xyz
ENV FS_WEB_ROOT www

#Override the following environment variables to let the service connect to different host names
ENV LOCALSTACK_ENDPOINT http://aws-localstack:4566
ENV STORAGE_DB_URL jdbc:postgresql://postgres/postgres
ENV PSQL_HOST postgres
ENV XYZ_HUB_REDIS_URI redis://redis
ENV HTTP_CONNECTOR_ENDPOINT http://xyz-http-connector:9090/psql
ENV HUB_ENDPOINT http://xyz-hub:8080/hub

COPY xyz-hub-service/target/xyz-hub-service.jar .
ADD xyz-util/src/main/resources/docker/Dockerfile /

EXPOSE 8080 9090
CMD java -jar xyz-hub-service.jar
