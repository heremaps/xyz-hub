![Naksha (नक्शा) - XYZ-Hub](xyz.svg)
---

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

[Naksha](https://en.wikipedia.org/wiki/Naksha) [(नक्शा)](https://www.shabdkosh.com/search-dictionary?lc=hi&sl=en&tl=hi&e=%E0%A4%A8%E0%A4%95%E0%A5%8D%E0%A4%B6%E0%A4%BE) is the name of this fork of the [XYZ-Hub](https://github.com/heremaps/xyz-hub) (pronounced **nakshaa** or **nakśā**). It stays a web service for the access and management of geospatial data. This spin-off was done to independently realize needed new features, not planned to be supported in the original [XYZ-Hub](https://github.com/heremaps/xyz-hub) project. The architecture was modified to allow extensions and plug-ins.

The meaning of [Naksha](https://en.wikipedia.org/wiki/Naksha)-Hub is “Map-Hub”.

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

Naksha uses [GeoJSON](https://tools.ietf.org/html/rfc79460) as the main geospatial data exchange format. Tiled data can also be provided as [MVT](https://github.com/mapbox/vector-tile-spec/blob/master/2.1/README.md).

# Prerequisites

* Java 17+
* Gradle 7.2+
* Postgres 10+ with PostGIS 2.5+

# Getting started
Clone and install the project using:

```bash
git clone https://github.com/heremaps/xyz-hub.git
cd xyz-hub
mvn clean install
```

### With docker

The service and all dependencies could be started locally using Docker compose.
```bash
docker-compose up -d
```

Alternatively, you can start freshly from the sources by using this command after cloning the project:
```bash
mvn clean install -Pdocker
```

*Hint: Postgres with PostGIS will be automatically started if you use 'docker-compose up -d' to start the service.*

### Without docker

The service could also be started directly as a fat jar. In this case Postgres and the other optional dependencies need to be started separately.

```bash
java -jar xyz-hub-service/target/xyz-hub-service.jar
```

### Configuration

The service persists out of modules with a bootstrap code to start the service. All configuration is done in the [config.json](here-naksha-app-service/src/main/resources/config.json).

The bootstrap code could be used to run only the `hub-verticle` or only the `connector-verticle` or it can be used to run both as a single monolith. In a microservice deployment you run one cluster with only `hub-verticle` deployment and another cluster with only `connector-verticle` deployment. It is as well possible to mix this, so running a monolith deployment that optionally can use connector configurations to use foreign connectors for individual spaces.

**Warning**: The `connector-verticle` does not perform security checks, so open it to external access will bypass all security restrictions!

The location of the configuration file could be modified using environment variables or by creating the `config.json` file in the corresponding configuration folder. The exact configuration folder is platform dependent, but generally follows the [XGD user configuration directory](https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html), standard, so on Linux being by default `~/.config/xyz-hub/`. For Windows the files will reside in the [CSIDL_PROFILE](https://learn.microsoft.com/en-us/windows/win32/shell/csidl?redirectedfrom=MSDN) folder, by default `C:\Users\{username}\.config\xyz-hub`. This path could be changed via environment variable `XDG_CONFIG_HOME`, which will result in the location `$XDG_CONFIG_HOME/xyz-hub/`. Next to this, an explicit location can be specified via the environment variable `XYZ_CONFIG_PATH`, this path will not be extended by the `xyz-hub` folder, so you can directly specify where to keep the config files. This is important when you want to start multiple versions of the service: `XYZ_CONFIG_PATH=~/.config/xyz-hub/a/ java -jar xyz-hub-service.jar`.

The individual environment variable names can be found in the source code of the configuration files being [CoreConfig](xyz-models/src/main/java/com/here/xyz/config/CoreConfig.java), [HubConfig](xyz-models/src/main/java/com/here/xyz/config/HubConfig.java) and [ConnectorConfig](xyz-models/src/main/java/com/here/xyz/config/ConnectorConfig.java). All properties annotated with `@JsonProperty` can always be set as well as environment variable, prefixed with `XYZ_` unless they are always starting with that prefix, for example `XYZ_HUB_REMOTE_SERVICE_URLS`. If the environment variable name is different you will find an additional annotation `@EnvName`. If the name within the configuration file is different, then either `@JsonProperty` or `@JsonName` annotations can be found.

```bash
mkdir ~/.config/xyz-hub
cp xyz-hub-service/src/main/resources/config.json ~/.config/xyz-hub/
cp xyz-hub-service/src/main/resources/config-db.json ~/.config/xyz-hub/
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

### OpenAPI specification

The OpenAPI specification files are accessible under the following URIs:
* Full: [http://{host}:{port}/hub/static/openapi/full.yaml](http://localhost:8080/hub/static/openapi/full.yaml)
* Stable: [http://{host}:{port}/hub/static/openapi/stable.yaml](http://localhost:8080/hub/static/openapi/stable.yaml)
* Experimental: [http://{host}:{port}/hub/static/openapi/experimental.yaml](http://localhost:8080/hub/static/openapi/experimental.yaml)
* Contract: [http://{host}:{port}/hub/static/openapi/contract.yaml](http://localhost:8080/hub/static/openapi/contract.yaml)
* Connector: [http://{host}:{port}/psql/static/openapi/openapi-http-connector.yaml](http://localhost:8080/psql/static/openapi/openapi-http-connector.yaml)

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