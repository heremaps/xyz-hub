/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */
package com.here.xyz.iceberg;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.here.xyz.XyzSerializable;
import com.here.xyz.events.Event;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.IcebergLocation;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.service.aws.lambda.SimulatedContext;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests the Lambda entry point of the connector: the stream handler reads the connector class from the {@code className}
 * connector param, as AWS Lambda calls it.
 */
class IcebergConnectorLambdaHandlerTest {
  private static final String NAMESPACE = "exports";
  private static final String TABLE = "space_lambda";

  @TempDir
  static Path warehouseDir;
  private static String warehouse;

  @BeforeAll
  static void writeTable() {
    warehouse = warehouseDir.toUri().toString();
    Catalog catalog = CatalogUtil.buildIcebergCatalog("writer", Map.of("type", "hadoop", "warehouse", warehouse), new Configuration());
    Table table = IcebergTestTables.createTable(catalog, TableIdentifier.of(Namespace.of(NAMESPACE), TABLE));
    IcebergTestTables.append(table, null, List.of(
        "{\"type\":\"Feature\",\"id\":\"p1\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[1,1]},\"properties\":{\"name\":\"p1\","
            + "\"@ns:com:here:xyz\":{\"version\":1,\"createdAt\":1700000000000,\"updatedAt\":1700000000000,\"author\":\"tester\"}}}"));
    IcebergTestTables.tag(table, "v1");
  }

  @Test
  void handlerDispatchesToTheIcebergConnector() throws Exception {
    XyzResponse response = invoke(event(TABLE, connectorParams(IcebergConnector.class.getName())));
    assertThat(response).isInstanceOf(FeatureCollection.class);
    assertThat(((FeatureCollection) response).getFeatures()).extracting(f -> f.getId()).containsExactly("p1");
  }

  @Test
  void unknownTableIsAnErrorResponseOfTheIcebergConnector() throws Exception {
    XyzResponse response = invoke(event("no_such_table", connectorParams(IcebergConnector.class.getName())));
    assertThat(response).isInstanceOf(ErrorResponse.class);
    assertThat(((ErrorResponse) response).getError()).isEqualTo(XyzError.NOT_FOUND);
  }

  @Test
  void defaultPsqlClassIsNotUsedAndNotOnTheClasspath() throws Exception {
    assertThatThrownBy(() -> Class.forName("com.here.xyz.psql.PSQLXyzConnector")).isInstanceOf(ClassNotFoundException.class);
    //Without className the handler would fall back to the PSQL connector, which fails here
    Map<String, Object> params = connectorParams(IcebergConnector.class.getName());
    params.remove("className");
    XyzResponse response = invoke(event(TABLE, params));
    assertThat(response).isInstanceOf(ErrorResponse.class);
    assertThat(((ErrorResponse) response).getError()).isEqualTo(XyzError.EXCEPTION);
  }

  private static Event<?> event(String table, Map<String, Object> connectorParams) {
    IterateFeaturesEvent<?> event = new IterateFeaturesEvent<>().withLimit(10);
    event.setParams(Map.of(IcebergLocation.EVENT_PARAM, Map.of(IcebergLocation.EVENT_PARAM_LOCATION,
        new IcebergLocation(NAMESPACE, table, "v1").toString())));
    event.setConnectorParams(connectorParams);
    event.setStreamId("iceberg-lambda-test");
    event.setSpace("test-space");
    return event;
  }

  private static Map<String, Object> connectorParams(String className) {
    Map<String, Object> params = new HashMap<>();
    params.put("connectorId", "iceberg");
    params.put("className", className);
    params.put("catalogName", "test");
    params.put("catalogProperties", Map.of("type", "hadoop", "warehouse", warehouse));
    return params;
  }

  private static XyzResponse invoke(Event<?> event) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    new IcebergConnector().handleRequest(new ByteArrayInputStream(event.serialize().getBytes(UTF_8)), out,
        new SimulatedContext("iceberg-lambda-test", Map.of()));
    return XyzSerializable.deserialize(out.toByteArray());
  }
}
