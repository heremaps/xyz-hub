# INSTRUCTION FOR NAKSHA API PERFORMANCE TEST SUITE

This test suite will perform a set of API calls in parallel against a running Naksha server and measure performance throughput. Currently employed APIs includes:

- GET features by tile ID `/hub/spaces/{spaceId}/tile/{type}/{tileId}`
- POST to patch one random feature from the previous call `/hub/spaces/{spaceId}/features`
- GET to query history of the patched feature `/hub/spaces/{spaceId}/search`

JMeter is required for this.

The test suite consists of:
- The JMeter script `naksha_local_load.jmx`.
- The file `tile_ids.csv` containing the tile IDs that will be used. The content can be modified as seen fit.

## Note: Manual setup

Currently the step to populate features for this test is not automated yet, and is already planned as a future work item. Hence this step needs to be performed manually once, so that the Naksha DB is ready.

Refer to the file `TEST_DATA_SETUP.md`, at the 'Setup' section, and follow the instruction.

## Execute the test

To run the test suite on the CLI, run:
```shell
jmeter -n \
-t naksha_local_load.jmx \
-l result-load-test.csv >> stats-output.txt
```

Additionally, many custom settings can be supplied through JMeter params. To add a parameter, simply use the syntax `-J` followed by the parameter name, and assign its value using the equal sign. Example:

```shell
jmeter -n \
-t naksha_local_load.jmx \
-JNAKSHA_TEST_STORAGE=load_test_storage \
-l result-load-test.csv >> stats-output.txt
```

## List of params

| Parameter name               | Default                               | Meaning                                                       |
|------------------------------|---------------------------------------|---------------------------------------------------------------|
| threads                      | 5                                     | Number of concurrent threads (users) to take part in the test |
| rampUp                       | 2                                     | Timeout in seconds for starting test threads                  |
| loopCount                    | 5                                     | How many time each thread must perform all test steps         |
| timeout                      | 1000                                  | Timeout in milliseconds for each HTTP request                 |
| NAKSHA_HOST_URL              | localhost                             |                                                               |
| NAKSHA_PROTOCOL              | http                                  |                                                               |
| NAKSHA_PORT                  | 8080                                  |                                                               |
| NAKSHA_JWT                   | {A valid signed JWT for Naksha local} |                                                               |
| RESOURCE_FILES_PATH          | {Current directory}                   |                                                               |
| PSQL_HOST                    | host.docker.internal                  |                                                               |
| PSQL_DB                      | postgres                              |                                                               |
| PSQL_USER                    | postgres                              |                                                               |
| PSQL_PASSWORD                | password                              |                                                               |
| PSQL_SCHEMA                  | naksha                                |                                                               |
| PSQL_PORT                    | 5432                                  |                                                               |
| NAKSHA_TEST_STORAGE          | ingest_test_storage                   |                                                               |
| NAKSHA_TEST_HANDLER          | ingest_test_handler                   |                                                               |
| NAKSHA_TEST_SPACE            | ingest_test_space                     |                                                               |
| NAKSHA_TEST_ACTIVITY_HANDLER | activity_history_test_handler         |                                                               |
| NAKSHA_TEST_ACTIVITY_SPACE   | activity_history_space_load_test      |                                                               |
