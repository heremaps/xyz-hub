/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.util.db.datasource;

import com.mchange.v2.c3p0.PooledDataSource;
import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;

public class StaticDataSources extends DataSourceProvider implements AutoCloseable {

  private final DataSource reader;
  private final DataSource writer;

  public StaticDataSources(DatabaseSettings dbSettings) {
    super(dbSettings);
    writer = createDataSource(false);
    reader = dbSettings.hasReplica() ? createDataSource(true) : writer;
  }

  public StaticDataSources(DataSource writer) {
    this(writer, writer);
  }

  public StaticDataSources(DataSource reader, DataSource writer) {
    super(null);
    this.reader = reader;
    this.writer = writer;
  }

  private DataSource createDataSource(boolean replica) {
    PGSimpleDataSource ds = new PGSimpleDataSource();
    ds.setServerNames(new String[] {replica ? dbSettings.getReplicaHost() : dbSettings.getHost()});
    ds.setDatabaseName(dbSettings.getDb());
    ds.setUser(replica ? dbSettings.getReplicaUser() : dbSettings.getUser());
    ds.setPassword(dbSettings.getPassword());
    ds.setApplicationName(dbSettings.getApplicationName());
    ds.setConnectTimeout(dbSettings.getDbCheckoutTimeout());
    ds.setCurrentSchema(dbSettings.getSchema());
    return ds;
  }

  @Override
  public DataSource getReader() {
    return reader;
  }

  @Override
  public DataSource getWriter() {
    return writer;
  }

  @Override
  public void close() throws Exception {
    if (writer instanceof PooledDataSource writer)
      writer.close();
    if (reader instanceof PooledDataSource reader && reader != writer)
      reader.close();
  }
}
