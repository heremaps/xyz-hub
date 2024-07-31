![Naksha (नक्शा) - XYZ-Hub](xyz.svg)
---

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

[Naksha](https://en.wikipedia.org/wiki/Naksha) [(नक्शा)](https://www.shabdkosh.com/search-dictionary?lc=hi&sl=en&tl=hi&e=%E0%A4%A8%E0%A4%95%E0%A5%8D%E0%A4%B6%E0%A4%BE) is the name of this fork of the [XYZ-Hub](https://github.com/heremaps/xyz-hub) (pronounced **nakshaa** or **nakśā**). It stays a web service for the access and management of geospatial data. This spin-off was done to independently realize needed new features, not planned to be supported in the original [XYZ-Hub](https://github.com/heremaps/xyz-hub) project. The architecture was modified to allow extensions and plug-ins.

The meaning of [Naksha](https://en.wikipedia.org/wiki/Naksha)-Hub is “Map-Hub”.

Naksha service, has been designed to primarily offer:
* GeoJson based REST API as service
* GeoJson based Java API as library (interacting directly with database)
* Support Postgres as default Storage system (with inbuilt transactional and history capabilities)
* Support publishing Data Change events (with atleast-once messaging guarantee)
* Additionally, Extension framework to provide ability:
    * to integrate REST APIs with your own custom Storage mechanism
    * to inject custom business logic as part of REST API request processing pipeline
    * to inject custom business logic as part of Data change event processing pipeline (subscription based)


# Overview

Naksha features are:
* Organize geo datasets in _collections_.
* Organize access to your data via _spaces_.
* Listen to changes done to your data via _subscriptions_.
* Store and manipulate individual geo features (points, line-strings, polygons).
* Retrieve geo features as vector tiles, with or without clipped geometries.
* Search for geo features spatially using a bounding box, radius, or any custom geometry.
* Implement own customer data processing logic as embedded handler or external extension handler.
* Explore geo features by filtering property values.
* Connect with your own data sources.
* Use default PostgresQL implementation with your own database.
* Build a real-time geo-data pipeline with processors.

Naksha uses [GeoJSON](https://www.rfc-editor.org/rfc/rfc7946.html) as the main geospatial data exchange format.

# Prerequisites

* Java 17+
* Gradle 7.2+
* Postgres 14+ with PostGIS 2.5+

# Getting started

Clone and install the project using:

```bash
git clone https://github.com/heremaps/naksha.git
cd naksha
gradle clean build
```

### Containerized Postgres & running with extensions

Naksha relies strongly on Postgres - you need running instance if you want to run Naksha locally.
Supported Postgres version is 14+ (the newer the better).
Apart from bare Postgres, Naksha needs [PostGIS]() extension which is mandatory. Apart from that, optional, but highly recommended extensions that are supported include: `pg_hint_plan` and `pg_stat_statements`.

You can use standalone instance installed directly on your host machine but there's also a docker image that hosts Postgres 16 with all the extensions mentioned above already installed. You can find its definition in [this Dockerfile](here-naksha-app-service/src/test/psql_container/Dockerfile).

To use the containerized Postgres with your locally runnning Naksha:
1) Navigate to [Dockerfile directory](here-naksha-app-service/src/test/psql_container):
   ```
   cd here-naksha-app-service/src/test/psql_container
   ```
2) Build the image:
   ```
   docker build --no-cache -t <IMAGE_ID> . 
   ```
3) Run the container (supplied options omit all auth - use it only locally, tweak if needed):
   ```
   docker run -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_INITDB_ARGS="--auth-host=trust --auth-local=trust" -e POSTGRES_HOST_AUTH_METHOD=trust <IMAGE_ID>
   ```
4) Now your database should be available on `localhost` with port `5432` - you can start Naksha the same way as described in [Run App](#run-app) section.

5) \[optional extension run\] Now you can run naksha jar and include [example config with additional extensions enabled](here-naksha-app-service/src/main/resources/test-config-with-extensions.json) like so:
```
java -jar build/libs/naksha-2.0.6-all.jar test-config-with-extensions 'jdbc:postgresql://localhost:5432/postgres?user=postgres&password=pswd&schema=naksha&app=naksha_local&id=naksha_admin_db'
```

### Run App

#### Running local fat jar

The service could also be started directly from a fat jar. In this case Postgres and the other optional dependencies need to be started separately.

To build the fat jar, at the root project directory, run one of the following:

```bash
# Using machine installed gradle (through apt, brew,... package managers)
gradle shadowJar
# Using gradle wrapper
./gradlew shadowJar
```

The jar can be found under `build/libs/`.

To ramp up Naksha with the jar, run:

```bash
java -jar <jar-file> <config-id> <database-url>

# Example 1 : Start service with test config against default Database URL (useful for local env)
java -jar build/libs/naksha-2.0.6-all.jar test-config
# Example 2 : Start service with given custom config and custom database URL (useful for cloud env)
java -jar build/libs/naksha-2.0.6-all.jar cloud-config 'jdbc:postgresql://localhost:5432/postgres?user=postgres&password=pswd&schema=naksha&app=naksha_local&id=naksha_admin_db'
# Example 3 : Start service with given custom config and default (local) database URL
java -jar build/libs/naksha-2.0.6-all.jar custom-config

```

Then use a web browser to connect to `localhost:8080`, an OK message should be displayed if the service is up and running.

#### Running Naksha in container

To run Naksha in container, take a look at [these docs](docker/README.md)

### OpenAPI specification

Once application is UP, the OpenAPI specification is accessible at `http(s)://{host}:{port}/hub/swagger/index.html`, by default at [http://localhost:8080/hub/swagger/index.html](http://localhost:8080/hub/swagger/index.html)


### Configuration

The service persists out of modules with a bootstrap code to start the service. Service provides default configuration in [default-config.json](here-naksha-lib-hub/src/main/resources/config/default-config.json).

The custom (external) configuration file can be supplied by modifying environment variable or by creating the `default-config.json` file in the corresponding configuration folder.
The exact configuration folder is platform dependent, but generally follows the [XGD user configuration directory](https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html), standard, so on Linux being by default `~/.config/naksha/v{x.x.x}/`. For Windows the files will reside in the [CSIDL_PROFILE](https://learn.microsoft.com/en-us/windows/win32/shell/csidl?redirectedfrom=MSDN) folder, by default `C:\Users\{username}\.config\naksha\v{x.x.x}\`.
Here `{x.x.x}` is the Naksha application version (for example, if version is `2.0.7`, then path will be `...\.config\naksha\v2.0.7`)

Next to this, an explicit location can be specified via the environment variable `NAKSHA_CONFIG_PATH`, this path will not be extended by the `naksha/v{x.x.x}` folder, so you can directly specify where to keep the config files. This is important when you want to start multiple versions of the service: `NAKSHA_CONFIG_PATH=~/.config/naksha/ java -jar naksha.jar {arguments}`.

In the custom config file, the name of the individual properties can be set as per source code here [NakshaHubConfig](here-naksha-lib-hub/src/main/java/com/here/naksha/lib/hub/NakshaHubConfig.java).
All properties annotated with `@JsonProperty` can be set in custom config file.

Config file is loaded using `{config-id}` supplied as CLI argument, as per following precedence on file location (first match wins):
1. using env variable `NAKSHA_CONFIG_PATH` (full path will be `$NAKSHA_CONFIG_PATH/{config-id}.json`)
2. as per user's home directory `user.home` (full path will be `{user-home}/.config/naksha/v{x.x.x}/{config-id}.json` )
3. as per config previously loaded in Naksha Admin Storage (PostgreSQL database)
4. default config loaded from jar (`here-naksha-lib-hub/src/main/resources/config/default-config.json`)

```bash
# Example of env variable NAKSHA_CONFIG_PATH

# First, copy default config to custom location
export NAKSHA_CONFIG_PATH=/my-location/naksha
cp here-naksha-lib-hub/src/main/resources/config/default-config.json $NAKSHA_CONFIG_PATH/

# Modify config as per need
vi $NAKSHA_CONFIG_PATH/default-config.json

# Start application using above config
java -jar naksha.jar default-config
```

# Usage

Start using the service by creating a _space_:

```bash
curl -H "content-type:application/json" \
-d '{"id": "test-space", "title": "my first space", "description": "my first geodata repo"}' \
"http://localhost:8080/hub/spaces"
```

The service will respond with the space definition including the space ID (should you not specify an own `id`):

```json
{
    "id": "test-space",
    "title": "my first space",
    "description": "my first geodata repo",
    "storage": {
        "id": "psql",
        "params": null
    },
    "owner": "ANONYMOUS",
    "createdAt": 1576601166681,
    "updatedAt": 1576601166681,
    "contentUpdatedAt": 1576601166681,
    "autoCacheProfile": {
        "browserTTL": 0,
        "cdnTTL": 0,
        "serviceTTL": 0
    }
}
```

You can now add _features_ to your brand new space:
```bash
curl -H "content-type:application/geo+json" -d '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[-2.960847,53.430828]},"properties":{"name":"Anfield","@ns:com:here:xyz":{"tags":["football","stadium"]},"amenity":"Football Stadium","capacity":54074,"description":"Home of Liverpool Football Club"}}]}' http://localhost:8080/hub/spaces/pvhQepar/features
```

The service will respond with the inserted geo features:
```json
{
    "type": "FeatureCollection",
    "etag": "b67016e5dcabbd5f76b0719d75c84424",
    "features": [
        {
            "type": "Feature",
            "id": "nf36KMsQAUYoM5kM",
            "geometry": {
                "type": "Point",
                "coordinates": [ -2.960847, 53.430828 ]
            },
            "properties": {
                "@ns:com:here:xyz": {
                    "space": "pvhQepar",
                    "createdAt": 1576602412218,
                    "updatedAt": 1576602412218,
                    "tags": [ "football", "stadium" ]
                },
                "amenity": "Football Stadium",
                "name": "Anfield",
                "description": "Home of Liverpool Football Club",
                "capacity": 54074
            }
        }
    ],
    "inserted": [
        "nf36KMsQAUYoM5kM"
    ]
}
```

# Testing locally

To run tests locally run Gradle `test` task:
```bash
./gradlew test
```

Code coverage report is generated with use of [jacoco](https://www.jacoco.org/)

To generate **subproject** level coverage, use Gradle task `jacocoTestReport`:

```bash
./gradlew test jacocoTestReport
```

Outputs for each subproject will be stored in `/[module]/build/reports/jacoco/test/html/index.html`

To generate **root** level aggregated coverage, use additional Gradle task `testCodeCoverageReport`:

```bash
./gradlew test jacocoTestReport testCodeCoverageReport
```

Outputs will be stored in `/build/reports/jacoco/testCodeCoverageReport/html/index.html`

To validate test coverage, run `jacocoTestCoverageVerification` Gradle task:
```bash
./gradlew test jacocoTestReport jacocoTestCoverageVerification
```




# Acknowledgements

XYZ Hub uses:

* [Vertx](http://vertx.io/)
* [Geotools](https://github.com/geotools/geotools)
* [JTS](https://github.com/locationtech/jts)
* [Jackson](https://github.com/FasterXML/jackson)

and [others](./pom.xml#L177-L479).

# Contributing

Your contributions are always welcome! Please have a look at the [contribution guidelines](CONTRIBUTING.md) first.

# License
Copyright (C) 2017-2022 HERE Europe B.V.

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](./LICENSE) file for details.
