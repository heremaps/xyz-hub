# Introduction

The team behind the [XYZ Hub](https://github.com/heremaps/xyz-hub) gratefully accepts contributions via
[pull requests](https://help.github.com/articles/about-pull-requests/) filed against the
[GitHub project](https://github.com/heremaps/xyz-hub/pulls).

# Signing each Commit

As part of filing a pull request we ask you to sign off the
[Developer Certificate of Origin](https://developercertificate.org/) (DCO) in each commit.
Any Pull Request with commits that are not signed off will be reject by the
[DCO check](https://probot.github.io/apps/dco/).

A DCO is lightweight way for a contributor to confirm that you wrote or otherwise have the right
to submit code or documentation to a project. Simply add `Signed-off-by` as shown in the example below
to indicate that you agree with the DCO.

An example signed commit message:

```
    README.md: Fix minor spelling mistake

    Signed-off-by: John Doe <john.doe@example.com>
```

Git has the `-s` flag that can sign a commit for you, see example below:

`$ git commit -s -m 'README.md: Fix minor spelling mistake'`

# Before making a pull-request

Eventually, before making a pull request, start the XYZ-Hub and the HTTP-connector and then execute 
the tests:

```bash
mvn clean install
java -jar xyz-hub-service/target/xyz-hub-service.jar >xyz-hub.txt 2>&1 &
java -cp xyz-hub-service/target/xyz-hub-service.jar com.here.xyz.hub.HttpConnector >xyz-http-connector.txt 2>&1 &
mvn verify -DskipTests=false
kill $(ps a | grep xyz-hub-service.jar | grep -E "[/]xyz-hub-service.jar" | grep -Eo "[0-9]+ pts" | grep -Eo "[0-9]+" | xargs) 2>/dev/null
```

**Note**: You need to have Redis installed locally.

# Run and test in IntelliJ IDEA

Open as project from existing code (`File->New->Project from Existing Sources...`) using `Maven`. Make copies of the configuration files:

```bash
mkdir .vertx
cp xyz-hub-service/src/main/resources/config.json .vertx/
cp xyz-hub-service/src/main/resources/connector-config.json .vertx/ 
```

Adjust the settings to your needs.

Create run profiles for the [XYZ-Hub Service](xyz-hub-service/src/main/java/com/here/xyz/hub/Core.java) and the [HTTP PSQL-Connector](xyz-hub-service/src/main/java/com/here/xyz/hub/HttpConnector.java), setting the environment variable `XYZ_CONFIG_PATH=$PROJECT_DIR$/.vertx/`. When running the tests in the IDE, ensure that the service and connector are running and that you provide the same environment variable to the tests.

If you do not run Redis, change the settings in the `config.json`:

```json
{
  "XYZ_HUB_REDIS_URI": "null",
  "DEFAULT_MESSAGE_BROKER": "Noop"
}
```
