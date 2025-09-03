FROM debian:trixie

LABEL maintainer="mnah@here.com"

ARG APP_VERSION=NA
ARG APP_BUILD_TIME=NA
ENV APP_NAME=devops-database
ENV SPLUNK_META_RELEASE=$APP_VERSION
ENV LOG_CONFIG=log4j2-docker.json

USER root

WORKDIR /opt/devops-database

COPY startScript.sh .
RUN chmod +x startScript.sh

COPY sample.input .
RUN chmod 600 sample.input

RUN mkdir /root/.aws
COPY aws.config.temp.txt /root/.aws/config
RUN chmod 600 /root/.aws/config
COPY aws.credentials.temp.txt /root/.aws/credentials
RUN chmod 600 /root/.aws/credentials

#EXPOSE 8080 80

RUN apt-get update && \
    apt-get -y install postgresql-client-17 git lsof ncat curl jq npm nodejs awscli vim apt-transport-https default-jre

# - Install Powershell -
#RUN curl -s -O https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb
#RUN dpkg -i packages-microsoft-prod.deb
#RUN rm packages-microsoft-prod.deb
#RUN apt-get update && \
#    apt-get install -y powershell

RUN npm install -g here-cicd --registry https://artifactory.in.here.com/artifactory/api/npm/here-node/

## ENTRYPOINT ["./moveSpaces.sh", "sample.input"]
ENTRYPOINT ["tail", "-f", "/dev/null"]

ADD devops.dockerfile /

