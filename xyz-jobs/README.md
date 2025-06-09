# XYZ Job Framework

A framework that can be used to perform long-running jobs (e.g., bulk import / export) on XYZ spaces.

## Steps to start the Job Framework locally

1. Start all dependent containers: `docker compose --file docker-compose-dynamodb.yml up -d --build --force-recreate postgres redis dynamodb aws-localstack`
2. Build & deploy the Job Step Lambda into the localstack by running the run-config `xyz-job-steps [install]`
3. Start the XYZ Hub Service by running the run-config `HubService`
4. Start the XYZ Job Service by running the run-config `JobService`
5. Optional run `CService`

Optionally: Start the JobPlayground by running `JobPlayground#main()`