/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

package com.here.xyz.hub.config.jdbc;

import com.here.xyz.httpconnector.CService;
import com.here.xyz.hub.Service;
import com.here.xyz.util.db.JdbcClient;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.db.datasource.PooledDataSources;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JDBCConfigClient extends JdbcClient {
  static final String SCHEMA = "xyz_config";
  private static final Logger logger = LogManager.getLogger();
  private String schema;
  private String table;

  private JDBCConfigClient(String schema, String table, DatabaseSettings dbSettings) {
    super(new PooledDataSources((dbSettings)));
    this.schema = schema;
    this.table = table;
  }

  public JDBCConfigClient(String schema, String table, com.here.xyz.hub.Config serviceConfiguration) {
    this(schema, table, getDatabaseSettings(serviceConfiguration));
  }

  public JDBCConfigClient(String schema, String table, com.here.xyz.httpconnector.Config serviceConfiguration) {
    this(schema, table, getDatabaseSettings(serviceConfiguration));
  }

  public static DatabaseSettings getDatabaseSettings(com.here.xyz.hub.Config serviceConfiguration) {
    if (serviceConfiguration == null)
      throw new NullPointerException("No Service configuration provided");
    return getDatabaseSettings(serviceConfiguration.STORAGE_DB_URL, serviceConfiguration.STORAGE_DB_USER,
        serviceConfiguration.STORAGE_DB_PASSWORD).withApplicationName(Service.XYZ_HUB_USER_AGENT);
  }

  public static DatabaseSettings getDatabaseSettings(com.here.xyz.httpconnector.Config serviceConfiguration) {
    if (serviceConfiguration == null)
      throw new NullPointerException("No CService configuration provided");
    return getDatabaseSettings(serviceConfiguration.STORAGE_DB_URL, serviceConfiguration.STORAGE_DB_USER,
        serviceConfiguration.STORAGE_DB_PASSWORD).withApplicationName(CService.USER_AGENT);
  }

  private static DatabaseSettings getDatabaseSettings(String dbUrl, String dbUser, String dbPassword) {
    if (dbUrl == null || dbUrl == null || dbPassword == null)
      throw new NullPointerException("STORAGE_DB_URL, STORAGE_DB_USER, STORAGE_DB_PASSWORD must be defined in Service.configuration "
          + "to use JDBCJobConfigClient.");
    URI dbUri = URI.create(dbUrl.substring(5));
    DatabaseSettings dbSettings = new DatabaseSettings("configClient")
        .withHost(dbUri.getHost())
        .withUser(dbUser)
        .withPassword(dbPassword)
        .withDbMaxPoolSize(4)
        .withDbAcquireRetryAttempts(1);

    if (dbUri.getPort() != -1)
      dbSettings.setPort(dbUri.getPort());

    String[] pathComponent = dbUri.getPath() == null ? null : dbUri.getPath().split("/");
    if (pathComponent != null && pathComponent.length > 1)
      dbSettings.setDb(pathComponent[1]);
    return dbSettings;
  }

  public SQLQuery getQuery(String queryText) {
    return new SQLQuery(queryText)
        .withVariable("schema", schema)
        .withVariable("table", table);
  }

  public Future<Void> init() {
    return initSchema();
  }

  public static <T> ResultSetHandler<List<T>> configListParser(Class<T> configType) {
    return rs -> {
      List<T> result = new ArrayList<>();
      while (rs.next())
        result.add(parseConfig(rs.getString("config"), configType));
      return result;
    };
  }

  public static <T> ResultSetHandler<T> configParser(Class<T> configType) {
    return rs -> rs.next() ? parseConfig(rs.getString("config"), configType) : null;
  }

  private static <T> T parseConfig(String configJson, Class<T> configType) {
    //TODO: Use XyzSerializable instead
    return Json.decodeValue(configJson, configType);
  }

  private Future<Void> initSchema() {
    return write(getQuery("CREATE SCHEMA IF NOT EXISTS ${schema}").withVariable("schema", schema))
        .onFailure(e -> logger.error("Can not create schema {}!", schema, e))
        .mapEmpty();
  }
}
