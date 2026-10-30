# Agent instructions for XYZ Hub

Follows the [agents.md](https://agents.md) convention.

Java 17, Maven multi-module. `xyz-hub-service` is the REST service, `xyz-psql-connector` the database layer,
`xyz-jobs` the import/export/copy jobs, `xyz-hub-test` the REST integration tests.

## Tests

Read [TESTING.md](TESTING.md) first.

- `-DskipTests=false` is mandatory. `xyz-hub-test` and both `xyz-jobs` modules set `skipTests` to `true` as a
  POM property, so without it they run nothing and still report success.
- Only `xyz-models`, `xyz-connectors` and `xyz-hub-service` run without containers.
- The express-mode import tests (`Express*IT`) need postgres plus `xyz-util`'s **test jar** — they reuse its
  FeatureWriter test framework, so run `mvn install -pl xyz-models,xyz-util -am -DskipTests=true` first. No
  lambda involved.
- The tasked step tests (`TaskedImport*`, `CopySpaceSteps*`, the export tests, `CreateIndex*`, `DropIndex*`,
  `CompressFiles*`, `S3MetricsCollector*`) do need `xyz-job-steps-fat.jar` deployed as a lambda. A test that
  waits is usually a missing or stale one, not a hang.

## Build

```bash
mvn clean install -DskipTests=true
mvn clean install -Pdocker -DskipTests=true -DdockerComposeFile=docker-compose-dynamodb.yml   # + images, stack
```

The `docker` profile binds `docker compose up` to `xyz-hub-service`'s `install` phase, so that module's build
time is mostly the image build.

## Modules

```
xyz-models ──┐
             ├─> xyz-util ─> xyz-psql-connector ─> xyz-job-steps ─> xyz-job-service ─> xyz-hub-service ─> xyz-hub-test
xyz-connectors ┘
```

`xyz-hub-service` depends on `xyz-job-service`, not the reverse. Only the first two modules can build
concurrently, so `mvn -T` gains nothing.

Shared test utilities live in `src/main/java`: `StepTestBase` and `JobTestBase` are used by both job modules.

## Conventions

`intellij-style.xml`: 2-space indent, 4-space continuation, 140 columns. Line comments without a space after
the slashes (`//like this`).

## Commits

`git commit -s` — every commit needs `Signed-off-by` or the DCO check fails. `git rebase --signoff` fixes a
range.

CI (`.github/workflows/test.yaml`) runs the test modules as a parallel matrix, each with its own stack,
aggregated into the one `test` check the branch protection requires.

## Writing tests

Integration tests run in parallel forks.

- Use unique resource IDs. `xyz-hub-test` appends `TestAuthenticator#TEST_SUFFIX`; the job modules use
  `getClass().getSimpleName() + "_" + randomAlpha(5)`. Hard-coded space, connector, tag or subscription names
  collide across forks.
- A test that lists spaces by tag or region still runs in parallel if the tag or region it filters by carries
  the fork suffix. A test counting state it does not own cannot: `owner=*` and `owner=others` include every
  other owner's shared spaces, `maxSpaces` is counted per owner hub-wide, and throttling sees all traffic.
  Those go into both the includes of `integration-test-exclusive` and the excludes of `integration-test` in
  `xyz-hub-test/pom.xml`.
- Clean up in `@AfterClass`. Leaked space tables accumulate until the suite slows down and then hangs.
