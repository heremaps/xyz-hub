# XYZ Hub Tests

## Steps to prepare local env to run tests

1. Start all dependent containers: 
```shell
  docker compose --file ../docker-compose-dynamodb.yml up -d --build --force-recreate postgres redis dynamodb aws-localstack
```
2. Build & deploy the Job Step Lambda into the localstack by running the run-config `xyz-job-steps [install]`
3. Start the XYZ Hub Service by running the run-config `HubService`
4. Start the XYZ Job Service by running the run-config `JobService`
5. Start the XYZ Connector service by running the run-config `CService`

## Troubleshooting

If you experience **500** or **403** errors when running tests in the master branch, it might be due to issues with your Docker volumes. Follow the steps below to reset your Docker environment:

1. **Shut down all running Docker containers associated with your compose files:**

```shell
  docker compose --file ../docker-compose-dynamodb.yml down
```

2. **Delete all Docker volumes:**

> **Warning:** This step will remove all Docker volumes on your system. Ensure that you do not need any of the data stored in these volumes before running this command.

```shell
   docker volume prune -f
```

After completing these steps, try setting up your env from scratch. 
This process ensures any corrupted or outdated volume data is removed, giving you a clean state for your Docker environment.
