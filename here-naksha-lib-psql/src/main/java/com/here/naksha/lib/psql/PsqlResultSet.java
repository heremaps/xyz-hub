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
package com.here.naksha.lib.psql;

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.storage.AbstractResultSet;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.postgresql.util.PGobject;

/**
 * The result reader implementation for PostgresQL. Expects that the result is ordered: ID, UUID, jsondata, geometry.
 */
public class PsqlResultSet<FEATURE extends XyzFeature> extends AbstractResultSet<FEATURE> {

  PsqlResultSet(@NotNull PreparedStatement stmt, @NotNull ResultSet rs, @NotNull Class<FEATURE> featureClass) {
    super(featureClass);
    this.stmt = stmt;
    this.rs = rs;
  }

  private PreparedStatement stmt;
  private ResultSet rs;

  private boolean loaded;
  private String id;
  private String uuid;
  private String jsondata;
  private String geometry;

  @Override
  public boolean next() {
    try {
      while (rs.next()) {
        try {
          this.id = rs.getString(1);
          this.uuid = rs.getString(2);
          final PGobject jsondata = (PGobject) rs.getObject(3);
          if (jsondata != null && "jsonb".equals(jsondata.getType()) && jsondata.getValue() != null) {
            this.jsondata = jsondata.getValue();
          } else {
            continue;
          }
          final PGobject geometry = (PGobject) rs.getObject(4);
          if (geometry != null && "geometry".equals(geometry.getType()) && geometry.getValue() != null) {
            this.geometry = geometry.getValue();
          }
          return loaded = true;
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }
    } catch (SQLException ignore) {
    }
    return loaded = false;
  }

  @Override
  public @NotNull String getId() {
    if (!loaded) {
      throw new NoSuchElementException();
    }
    return id;
  }

  @Override
  public @NotNull String getUuid() {
    if (!loaded) {
      throw new NoSuchElementException();
    }
    return uuid;
  }

  @Override
  public @NotNull String getJson() {
    if (!loaded) {
      throw new NoSuchElementException();
    }
    return jsondata;
  }

  @Override
  public @Nullable String getGeometry() {
    if (!loaded) {
      throw new NoSuchElementException();
    }
    return geometry;
  }

  @Override
  public @NotNull FEATURE getFeature() {
    return featureOf(getJson(), getGeometry());
  }

  @Override
  public void close() {
    if (rs != null) {
      try {
        rs.close();
      } catch (SQLException ignore) {
      }
      try {
        stmt.close();
      } catch (SQLException ignore) {
      }
      rs = null;
      stmt = null;
    }
  }
}
