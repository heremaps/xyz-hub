# Testing

Unit tests run by surefire (`*Test`), integration test  e bulk
of the suite and need Docker containers — postgres at minimum, the whole stack for the REST tests.

## Quick start

Set the stack up as described under [Everything running](#5-everything-running), then:

```bash
mvn -o clean verify -DskipTests=false        # everything, roughly 6 minutes
```

- **`-DskipTests=false` is mandatory.** `xyz-hub-test` and both `xyz-jobs` modules set `skipTests` to `true` as
  a POM property, which surefire and failsafe both honour. Without it those modules run nothing and the build
  still reports success.
- **`-o`** is offline mode, used throughout below. Optional, but faster once `~/.m2` is warm.
- **`-Dmaven.test.failure.ignore=true`** gives every module's result instead of stopping at the first failure.

## Where the tests live

| Module | Tests | Needs |
|---|---|---|
| `xyz-models`, `xyz-connectors` | unit | nothing |
| `xyz-hub-service` | unit | nothing |
| `xyz-psql-connector` | 15 ITs | postgres |
| `xyz-util` | SQL FeatureWriter suites, SQL helpers | postgres |
| `xyz-util` | `Hub*TestSuiteIT` | full stack |
| `xyz-jobs/xyz-job-steps` | `Express*IT` (express mode import) | postgres + `xyz-util` test jar |
| `xyz-jobs/xyz-job-steps` | tasked step tests | postgres, localstack, step lambda |
| `xyz-jobs/xyz-job-service` | 7 ITs | full stack |
| `xyz-hub-test` | 58 REST ITs | full stack |

## The five categories

Each needs everything the ones above it need.

### 1. Unit tests — nothing running

```bash
mvn -o test -DskipTests=false -pl xyz-models,xyz-connectors,xyz-hub-service
```

### 2. Postgres only

```bash
docker compose -f docker-compose-dynamodb.yml up -d postgres
mvn -o verify -DskipTests=false -pl xyz-psql-connector
```

Also `xyz-util`'s `SQL*TestSuiteIT`, `SQLQueryIT`, `SQLQueryLockIT`, `SQLScriptsIT`, `XyzSpaceTableHelperIT` and
`GeometryValidatorIT` — the PLV8 FeatureWriter suites and the SQL helpers. Its `Hub*TestSuiteIT` classes go
through `HubWebClient`, so running all of `xyz-util` needs the full stack.

### 3. Express mode import — postgres plus the xyz-util test jar

The express writer is verified against the same scenarios as the PLV8 FeatureWriter, using the framework in
`xyz-util`'s **test jar** (`com.here.xyz.test.featurewriter.TestSuite` / `SpaceWriter`), which must be installed
first.

```bash
docker compose -f docker-compose-dynamodb.yml up -d postgres
mvn -o install -pl xyz-models,xyz-util -am -DskipTests=true
mvn -o verify -pl xyz-jobs/xyz-job-steps -DskipTests=false \
    -Dit.test='Express*IT' -Dtest='ExpressRoutingGuardTest' \
    -DfailIfNoSpecifiedTests=false -Dsurefire.failIfNoSpecifiedTests=false
```

No lambda, localstack or hub. Both `failIfNoSpecifiedTests` flags are needed because the selection matches only
failsafe classes on one side and only a surefire class on the other.

### 4. Tasked steps — postgres, localstack and the deployed step lambda

These steps finalize their task items through a lambda callback from the database, so `xyz-job-steps-fat.jar`
must be built *and* deployed.

```bash
mvn -o clean install -Pdocker -DskipTests=true -DdockerComposeFile=docker-compose-dynamodb.yml

AWS_ACCESS_KEY_ID=localstack AWS_SECRET_ACCESS_KEY=localstack AWS_DEFAULT_REGION=us-east-1 \
  ./xyz-jobs/xyz-job-steps/src/main/bash/deployLocalLambda.sh

mvn -o verify -DskipTests=false -pl xyz-jobs/xyz-job-steps
```

The affected classes call `sendLambdaStepRequestBlock` or `waitTillTaskItemsAreFinalized`:
`TaskedImportStepTest`, `CopySpaceStepsTest`, `ExportTestBase` and its subclasses, `CreateIndexStepTest`,
`DropIndexStepTest`, `CompressFilesStepTest`, `S3MetricsCollectorStepTest`.

**A missing or stale lambda makes them wait, not fail.** Redeploy after any change under
`xyz-jobs/xyz-job-steps/src/main`.

### 5. Everything running

`xyz-hub-test` and `xyz-jobs/xyz-job-service` need hub, job-service, postgres, dynamodb, redis and localstack.

```bash
mvn -o clean install -Pdocker -DskipTests=true -DdockerComposeFile=docker-compose-dynamodb.yml

AWS_ACCESS_KEY_ID=localstack AWS_SECRET_ACCESS_KEY=localstack AWS_DEFAULT_REGION=us-east-1 \
  ./xyz-jobs/xyz-job-steps/src/main/bash/deployLocalLambda.sh

cd xyz-jobs/xyz-job-service/src/main/bash && docker run --rm --entrypoint '' \
  -v ./localSetup.sh:/aws/localSetup.sh --add-host host.docker.internal=host-gateway \
  amazon/aws-cli ./localSetup.sh true && cd -

mvn -o verify -DskipTests=false -pl xyz-hub-test
```

Both should answer `200` before you start:

```bash
curl -s -o /dev/null -w 'hub %{http_code}\n' http://localhost:8080/hub/spaces
curl -s -o /dev/null -w 'jobs %{http_code}\n' http://localhost:7070/jobs
```

## Narrowing a run

```bash
# one surefire class
mvn -o test -DskipTests=false -pl xyz-jobs/xyz-job-steps \
  -Dtest=CopySpaceStepsTest -DfailIfNoSpecifiedTests=false

# specific ITs, skipping the build phases
mvn -o failsafe:integration-test failsafe:verify -DskipTests=false -pl xyz-hub-test \
  -Dit.test='ReadFeatureApiIT,UpdateFeatureApiIT'

# serial, when a parallel run is suspected of interfering
mvn -o verify -DskipTests=false -pl xyz-hub-test -Dit.forkCount=1
```

## Parallelism

| Property | Default | Meaning |
|---|---|---|
| `it.forkCount` | 4 | JVMs failsafe uses for IT classes. `1` restores serial execution. |
| `it.threadCount` | 4 | Threads per JVM for classes annotated `@Execution(CONCURRENT)`. |
| `shade.skip` | false | Skip repackaging the fat jars. Safe once they are built and the lambda is deployed. |

CI runs the test modules as a matrix of parallel jobs, each with its own stack, aggregated into the one
`test` check the branch protection requires. See `.github/workflows/test.yaml`.

## Writing tests

**Address resources by a unique ID.** `xyz-hub-test` appends `TestAuthenticator#TEST_SUFFIX` to space IDs; the
job modules use `getClass().getSimpleName() + "_" + randomAlpha(5)`. A hard-coded space, connector, tag or
subscription name collides across forks.

**Filter a hub-wide listing by something fork local.** A test that lists spaces by tag or region can still run
in parallel, as long as the tag or region it filters by carries the fork suffix, so the listing cannot return
another fork's spaces. The same trick covers the connector listing, because a connector ID can be fork local
too.

**A test counting state it does not own cannot run in parallel.** No naming isolates an exact count over
`owner=*` or `owner=others`, because those listings include every other owner's shared spaces, nor the
`maxSpaces` JWT limit, which the hub counts per owner across the whole hub, nor the request throttling, which
any other traffic perturbs. Such classes belong in the `integration-test-exclusive` includes *and* the
`integration-test` excludes in `xyz-hub-test/pom.xml` — miss one and it runs twice.

**Clean up in `@AfterClass`.** Leaked space tables accumulate until the suite slows down and then hangs.

**Reuse the base classes**: `TestSpaceWithFeature` in `xyz-hub-test`, `StepTestBase` and `JobTestBase` for the
job modules. The latter two live in `src/main/java` because both job modules share them.

## Reading the results

```bash
# per-class timings, slowest first
for f in */target/*-reports/TEST-*.xml; do
  python3 - "$f" <<'PY'
import re,sys
t=open(sys.argv[1],encoding='utf-8',errors='replace').read()
m=re.search(r'<testsuite[^>]*name="([^"]*)"[^>]*time="([\d.,]+)"',t)
if m: print(f'{float(m.group(2).replace(",","")):8.1f}s  {m.group(1)}')
PY
done | sort -rn | head -20
```

`target/failsafe-reports/failsafe-summary.xml` is appended to across runs, so an earlier failure fails `verify`
again. Use `clean`, or delete the directory first.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| "Tests run: 0", or a module is skipped | Missing `-DskipTests=false`. |
| Hangs repeating `N Threads are not finished!` | A task item was never finalized — its lambda callback was lost. Fails after a 5 minute deadline. Usually the lambda is not deployed, or the machine is overloaded; reduce `-Dit.forkCount`. On an idle machine, suspect the callback path rather than the test. |
| `function max_bigint() does not exist`, or the hub returns `502 Connector error` | The hub side SQL scripts were never installed. Happens when postgres is recreated alone (`up --force-recreate -V postgres`) while the hub keeps running: the installer sees `hub.common`, skips, and leaves `hub.ext` missing. Recreate the whole stack: `docker compose -f docker-compose-dynamodb.yml down -v`, then the setup steps above. |
| Express import tests wait, then `NoCredentialsError` | The `aws_s3.*` settings come from the postgres image's init script, which only runs on a fresh data directory. A restart is not enough — the volume has to go: `docker compose -f docker-compose-dynamodb.yml down -v`. |
| Suite slows down over days, then hangs | Orphaned space tables. `docker exec postgres psql -U postgres -d postgres -c "select count(*) from pg_tables where schemaname='public';"` — a clean stack has 1. Once it reaches the thousands, recreate the stack with `down -v`. |
| A `xyz-hub-service/src/main/resources/connectors.json` change has no effect | The hub only seeds connectors into DynamoDB when the entry is absent. `aws dynamodb delete-table --endpoint-url http://localhost:8000 --table-name xyz-hub-local-connectors`, then restart the hub. Verify with `curl -s localhost:8080/hub/connectors/psql`. |
| `LimitsTestIT` fails with 403 creating a space | Its JWT carries `maxSpaces: 1`, counted across all of that owner's spaces, so nothing else may create one concurrently. It belongs in the serial execution. |
| Resources vanish mid-run | An IDE or file watcher rewriting `target/` shows up as `NullPointerException` from `getResourceAsStream`. Run from a separate git worktree to rule it out. |
