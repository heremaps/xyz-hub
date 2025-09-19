FROM openjdk:17-slim

LABEL maintainer="mnah <mnah@here.com>"

ENV LOG_CONFIG=log4j2-console-plain.json
ENV LOG_PATH=/var/log/xyz

USER root
WORKDIR /opt/EcsTaskStep

#Override the following environment variables to let the service connect to different host names
#ENV LOCALSTACK_ENDPOINT=http://aws-localstack:4566
#ENV STORAGE_DB_URL=jdbc:postgresql://postgres/postgres
#ENV PSQL_HOST=postgres
#ENV XYZ_HUB_REDIS_URI=redis://redis
#ENV HTTP_CONNECTOR_ENDPOINT=http://xyz-http-connector:9090/psql
#ENV HUB_ENDPOINT=http://xyz-hub:8080/hub

COPY xyz-jobs/xyz-job-steps/target/xyz-job-steps-fat.jar .
ADD xyz-jobs/xyz-job-steps/src/main/docker/ecs.java.dockerfile /

#COPY ./xyz-job-steps-fat.jar .
#ADD ./ecs.java.dockerfile /

ENTRYPOINT [ "java","-cp","xyz-job-steps-fat.jar","com.here.xyz.jobs.steps.execution.EcsTaskStep"]
