![XYZ Hub](xyz.svg)
---

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Run XYZ Hub tests](https://github.com/heremaps/xyz-hub/workflows/Run%20XYZ%20Hub%20tests/badge.svg)](https://github.com/heremaps/xyz-hub/actions?query=workflow%3A%22Run+XYZ+Hub+tests%22)

XYZ Hub is a RESTful web service for the access and management of geospatial data.

# Overview
Some of the features of XYZ Hub are:
* Organize geo datasets in _spaces_
* Store and manipulate individual geo features (points, linestrings, polygons)
* Retrieve geo features as vector tiles, with or without clipped geometries
* Search for geo features spatially using a bounding box, radius, or any custom geometry
* Explore geo features by filtering property values
* Retrieve statistics for your _spaces_
* Analytical representation of geo data as hexbins with statistical information
* Connect with different data sources
* Build a real-time geodata pipeline with processors
* Attach listeners to react on events

You can find more information in the [XYZ Documentation](https://www.here.xyz/api) and in the [OpenAPI specification](https://xyz.api.here.com/hub/static/redoc/index.html).

XYZ Hub uses [GeoJSON](https://tools.ietf.org/html/rfc79460) as the main geospatial data exchange format. Tiled data can also be provided as [MVT](https://github.com/mapbox/vector-tile-spec/blob/master/2.1/README.md).

# Prerequisites

 * Java 8.x
 * Maven 3.6+
 * Postgres 10+ with PostGIS 2.5+
 * Redis 5+ (optional)
 * Docker 18+ (optional)
 * Docker Compose 1.24+ (optional)

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
java [OPTIONS] -jar xyz-hub-service/target/xyz-hub-service.jar
```

Example:

```bash
java -DHTTP_PORT=8080 -jar xyz-hub-service/target/xyz-hub-service.jar
```

### Configuration options
The service start parameters could be specified by editing the [default config file](./xyz-hub-service/src/main/resources/config.json), using environment variables or system properties. See the default list of [configuration parameters](https://github.com/heremaps/xyz-hub/wiki/Configuration-parameters) and their default values.

Note: To override the configuration files from the JAR, place the configuration file (`config.json` and/or `connector-config.json`) into the home directory of the subdirectory `.xyz-hub` of the user running the service. Example in Linux:

```bash
mkdir ~/.xyz-hub
cp xyz-hub-service/src/main/resources/config.json ~/.xyz-hub
cp xyz-hub-service/src/main/resources/connector-config.json ~/.xyz-hub 
```

# Usage

Start using the service by creating a _space_:

```bash
curl -H "content-type:application/json" -d '{"title": "my first space", "description": "my first geodata repo"}' http://localhost:8080/hub/spaces
```

The service will respond with the space definition including the space ID:

```json
{
    "id": "pvhQepar",
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
* Stable: `http://<host>:<port>/hub/static/openapi/stable.yaml`
* Experimental: `http://<host>:<port>/hub/static/openapi/experimental.yaml`

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