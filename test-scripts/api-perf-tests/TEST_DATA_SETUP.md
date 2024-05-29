# Local Naksha load test

This README should be treated as nothing more than a scratch file with some results and instructions
w.r.t. how to load test Naksha in local containerized environment.

To run test for your own refer to sections:

- [Setup](#setup)
- [Running tests](#running-tests)

## My results

This section summarizes mine (Jakub's) results etc.

### Tests on infra setup #1

Resources:
- podman machine cpus: 8
- podman machine memory: 4096
- naksha-app cpus: 4 
- naksha-psql cpus: 4
- naksha-app memory: 2048
- naksha-psql memory: 2048
- psql max connections: 100
- tiles: 256
- features: 20_000

JMeter tests:

| thread count | loop count | ramp up | timeouts (s) | outcome                                                                     |
|--------------|------------|---------|--------------|-----------------------------------------------------------------------------|
| 20           | 20         | 2       | 1            | ratio: 30.9/s, avg: 558, min: 154, max: 1207, errors: 0                     |
| 40           | 20         | 2       | 1            | ratio: 23.5/s, avg: 1572, min: 145, max: 6294, errors: 0                    |
| 80           | 20         | 2       | 1            | errors: 1600 (100%), reason:  timeout + OOMKilled                           |
| 80           | 20         | 2       | 3            | errors: 1597 (99.81%), reason:  timeout + OOMKilled                         |
| 60           | 20         | 2       | 3            | errors: 706 (58.83%), reason: timeout, no point in analyzing successful 41% |
| 50           | 20         | 2       | 3            | errors: 176 (17.60%), reason: timeout, no point in analyzing successful 82% |

The last (failing) scenarios (for 80, 60, 50 threads) from table above was run multiple times on fresh instances to ensure
repeatability
- JMeter was able to send *some* request, each of them took Naksha more than 1 second to respond
  so JMeter treated this as failure (1s timeout is very generous, not defining any will cause JMeter
  to simply hang)
- After *some* responding to *some* request, Naksha got killed by docker engine as it consumer too
  mach memory (verified with `docker inspect`)
- I changed timeout on JMeter's side to 3s (!) - it did not help, only 3 out of 1600 planned request
  returned successfully

Conclusion: 
- with this setup of infra (see above), `naksha-app` was hitting its limits at ~40 concurrent clients
- the different between 20 and 40 concurrent clients is very noticable
- next tests with larger resources (cpu -> 8, memory-wise we are fine) will be concluded


### Test on infra setup #2:

Results for small instances
- 10 features per tile, 256 tiles
- app: 4 cpus, 4096 mem
- psql: 4 cpus, 4096 mem

| concurrency | loops per thread | timeout (ms) | summary                                                                                                  | errors                                                                                            |
| ----------- | ---------------- | ------------ | -------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| 80          | 10               | 1000         | summary = 800 in 00:00:15 = 53.6/s<br>Avg: 628<br>Min: 31<br>Max: 13279<br>Err: 1 (0.12%)<br>            | single `500 Internal Server error`                                                                |
| 80          | 10               | 2000         | summary = 800 in 00:00:06 = 138.1/s<br>Avg: 381<br>Min: 33<br>Max: 883<br>Err: 0 (0.00%)                 | -                                                                                                 |
| 100         | 5                | 1000         | summary = 500 in 00:00:20 = 25.1/s<br>Avg: 896<br>Min: 33<br>Max: 19440<br>Err: 22 (4.40%)               | 22 responses with `500 Internal Server error`                                                     |
| 100         | 10               | 2000         | summary = 1000 in 00:00:23 = 42.7/s<br>Avg: 795<br>Min: 22<br>Max: 18445<br>Err: 60 (6.00%)<br>          | 60 responses with `500 Internal Server error`<br>                                                 |
| 100         | 20               | 3000         | (stats omitted on purpose)<br><br>Thread group breakdown:<br>Active: 99 <br>Started: 100 <br>Finished: 1 | This iteration was hanging and killed manually (99 of 100 thread were unable to finish its loops) |
|             |                  |              |                                                                                                          |                                                                                                   |

Results for big instances
- 10 features per tile, 256 tiles
- app: 12 cpus, 4096 mem
- psql: 4 cpus, 4096 mem

| concurrency | loops per thread | timeout (ms) | summary                                                                                            | errors                                         |
| ----------- | ---------------- | ------------ | -------------------------------------------------------------------------------------------------- | ---------------------------------------------- |
| 100         | 5                | 1000         | summary = 500 in 00:00:04 = 115.9/s<br>Avg: 470<br>Min: 64<br>Max: 1100<br>Err: 0 (0.00%)<br>      | -                                              |
| 100 (12:23) | 10               | 1000         | summary = 1000 in 00:00:18 = 55.7/s<br>Avg: 1582<br>Min: 31<br>Max: 10678<br>Err: 46 (4.60%)       | 46 responses with `500 Internal Server error`  |
| 100 (13:01) | 10               | 1000         | summary = 1000 in 00:00:16 = 62.7/s<br>Avg: 1379<br>Min: 30<br>Max: 8739<br>Err: 18 (1.80%)        | 18 responses with `500 Internal Server error`  |
| 100 (13:02) | 10               | 1000         | summary = 1000 in 00:00:10 = 104.4/s<br>Avg: 737<br>Min: 36<br>Max: 2212<br>Err: 8 (0.80%)         | 8 responses with `500 Internal Server error`   |
| 200         | 5                | 1000         | summary = 1000 in 00:00:08 = 131.9/s<br>Avg: 1086<br>Min: 45<br>Max: 2523<br>Err: 298 (29.80%)<br> | 298 responses with `500 Internal Server error` |
| 200         | 5                | 1000         | summary = 1000 in 00:00:26 = 38.4/s<br>Avg: 1515<br>Min: 32<br>Max: 17416<br>Err: 561 (56.10%)     | 561 responses with `500 Internal Server error` |
| 200         | 5                | 1000         | summary = 1000 in 00:00:24 = 41.4/s<br>Avg: 3844<br>Min: 42<br>Max: 22588<br>Err: 555 (55.50%)     | 555 responses with `500 Internal Server error` |
| 200         | 5                | 1000         | summary = 1000 in 00:00:21 = 47.3/s<br>Avg: 3217<br>Min: 43<br>Max: 16216<br>Err: 543 (54.30%)     | 543 responses with `500 Internal Server error` |

## Setup

### PSQL container

```shell
docker run \
  -p 5432:5432 \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_INITDB_ARGS="--auth-host=trust --auth-local=trust" \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  -v "/Users/amanowic/dev/load_psql/data/:/var/lib/postgresql/data" \
  --name=naksha-psql \
  docker.io/postgis/postgis:16-3.4
```

Mind the volume mount (`-v`) - we will populate our database once with 20k features, if you won't
use shared volume, db data will need to be populated for each container psql lifecycle. Using volume
will save you lots of time (no need to repopulate db even if your config will change).

### Build and run Naksha container

Please refer to the `docker/README.md` file.

### Load fake data into PSQL

#### Create storage, handler & space

Storage (mind the `host`):

```shell
curl -XPOST localhost:8080/hub/storages -H "content-type: application/json" -d '
{
  "id": "ingest_test_storage",
  "type": "Storage",
  "title": "Test PSQL storage",
  "description": "PSQL storage instance for testing purpose",
  "className": "com.here.naksha.lib.psql.PsqlStorage",
  "properties": {
    "master": {
      "host": "host.docker.internal",
      "db": "postgres",
      "user": "postgres",
      "password": "password",
      "readOnly": false
    },
    "appName": "ingest-test",
    "schema": "naksha"
  }
}
' -v
```

Handler:

```shell
curl -XPOST localhost:8080/hub/handlers -H "content-type: application/json" -d '
{
  "id": "ingest_test_handler",
  "type": "EventHandler",
  "title": "Storage Handler for UniMap Moderation Dev Storage",
  "description": "Default Naksha Storage Handler for operations on UniMap Moderation Dev Storage",
  "className": "com.here.naksha.lib.handlers.DefaultStorageHandler",
  "active": true,
  "extensionId": null,
  "properties": {
    "storageId": "ingest_test_storage",
    "autoDeleteCollection": false
  }
}
' -v
```

Space:

```shell
curl -XPOST localhost:8080/hub/spaces -H "content-type: application/json" -d '
{
  "id": "ingest_test_space",
  "type": "Space",
  "title": "Topology Space for UniMap Moderation Dev Storage",
  "description": "Space for managing Topology Feature collection in UniMap Moderation Dev Storage",
  "eventHandlerIds": [
    "ingest_test_handler"
  ],
  "properties": {
  }
}
' -v
```

Activity history handler (not needed, skip to [features creation](#populate-the-space-with-fake-data):
```shell
curl -XPOST localhost:8080/hub/handlers -H "content-type: application/json" -d '
{
  "id": "activity_history_test_handler",
  "type": "EventHandler",
  "title": "Sample Activity History Handler",
  "description": "Activity History Handler used for tests",
  "className": "com.here.naksha.handler.activitylog.ActivityLogHandler",
  "active": true,
  "extensionId": null,
  "properties": {
    "spaceId": "ingest_test_space"
  }
}
' -v
```

Activity history space (not needed, skip to [features creation](#populate-the-space-with-fake-data):
```shell
curl -XPOST localhost:8080/hub/spaces -H "content-type: application/json" -d '
{
  "id": "activity_history_space_load_test",
  "type": "Space",
  "title": "Activity space for performance test using jmeter",
  "description": "Activity space for performance test using jmeter",
  "eventHandlerIds": [
    "activity_history_test_handler"
  ]
}
' -v
```

#### Populate the space with fake data

Run `ingestRandomFeatures` method
from `com.here.naksha.app.data.GenerativeDataIngest.GenerativeDataIngest`.\
It generates 10 features per `tileId`.

Take a look at the `ingest_data/topology/tile_ids.csv` file - it contains all tiles for which our
features were generated (generator logic takes `tileId` as arg and creates matching geometry
for feature to contain).

The latest JMeter test suite already includes ramping up and tearing down all Naksha resources, EXCEPT for populating these features for the first time. After this populating step, you can run the suite once, with the `setUp` thread group in mode `Continue` while encountering Sampler error, to clean up all the resources like Naksha storage, handlers, and spaces. Then toggle the mode back to `Stop test`. From now on the suite can just be executed directly, without worrying about setting up the resources above. A future work will solve this issue, by including this features populating step into the jmeter suite.
#### Prepare your  JMeter scenario

1) If you don't have JMeter, install it: `brew install jmeter`
2) Open up our scenario: `jmeter -t naksha_local_load.jmx`
3) Navigate to `CSV Tile ids` step and update `Filename` so it will point
   to `ingest_data/topology/tile_ids.csv` file from test resources (use full path)

## Running tests

### Before you run tests

1) Be sure that you completed every step from [the setup](#setup).\
2) [optional] You can also spawn docker's statistics on the side to see how much pressure your
   containers get (do that before running JMeter):
    ```shell
    docker stats --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"
    ```

### Running the scenario

Main command looks as follows:

```shell
jmeter -n \
 -t naksha_local_load.jmx \
  -Jthreads=20 \
  -JrampUp=2 \
  -JloopCount=20 \
  -Jtimeout=1000 \
  -l {your_results_file} >> {your_output_file}
```

Parameters explanation:

- Jthreads: how many concurrent client should JMeter use when sending request to Naksha (aka:
  concurrency)
- JrampUp: ramp up value of JMeter's thread group (aka: how long before they start doing stuff)
- JloopCount: how many request should each client send
- JTimeout: timeout of request sent to Naksha
- result file: csv with metadata regarding the execution (latency, URL that was used, response code
  etc)
- output file: summary of execution (avg time per thread etc)

### Working with helper script

There's a small bash script: [test_naksha.sh](test_naksha.sh) which aims to ease the process of the jmeter load testing.\
Please have in mind that this was just a local tool tailored for specific environment, treat it as such.\

What it does:
1) Triggers jmeter scenario
2) Stores the output and summary in dedicated run-directory
3) Dumps the logs of the app in dedicated run-directory
4) Dumps the logs of the psql in dedicated run-directory

Args:
- `c`: concurrency, mapped to `Jthreads`, required
- `t`: timout, mapped to `JTimeout`, required
- `r`: ramp up, mapped to `JrampUp`, optional, 2 by default
- `l`: loop count, mapped to `JloopCount`, optional, 20 by default

This was written because I had to run different configuration multiple and compare the runs against each other from different angles.\

There are some fixed values references that this script expects (this can be extracted to script arguments):
1) The jmeter script: `naksha_local_load.jmx`
2) The name of naksha-app container: `naksha-app`
3) The name of naksha-psql container: `naksha-psql`

Mind the fact that for long running tests, output logs might get heavy ;)

Sample usage:
```shell
./test_naksha.sh -c 180 -t 1000 -l 10
```
This would 
* run jmeter with 180 threads, each performing 10 requests, each request with 1000ms socket timeout
* store all of the output files in `c=180-t=1000-r=2-l=10-{date}` directory:
    * `output`: file with jmeter execution summary
    * `results.csv`: files with individual execution summary
    * `naksha-app.logs`: application logs gathered during jmeter execution
    * `naksha-psql.logs`: db logs gathered during jmeter execution


### Helper scratchpad

Verify distinct features loaded:

```shell
SELECT count(distinct jsondata ->> 'id')
FROM naksha.ingest_test_space;
```

PSQL connection limit:

```shell
show max_connections;
```

PSQL currently opened connections:

```shell
SELECT COUNT(*) from pg_stat_activity;
```

more detailed view on opened connections:

```shell
SELECT * FROM pg_stat_activity;
```

Analyzing `output` file:
```shell
grep "summary =" output | sed 's/ Avg:/\nAvg:/g; s/ Min:/\nMin:/g; s/ Max:/\nMax:/g; s/ Err:/\nErr:/g' | tr -s ' '
```

Get `ERROR` lines from app log:
```shell
grep "\[ERROR\]" naksha-app.logs 
```

Unique errors from app log:
```shell
grep "\[ERROR\]" naksha-app.logs | awk -F' - ' '{print $3}' | uniq
```

How many socket timeout related errors:
```shell
grep "Caused by: java.net.SocketTimeoutException" naksha-app.logs | wc -l
```
