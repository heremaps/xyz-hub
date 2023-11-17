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

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.storage.EExecutedOp;
import com.here.naksha.lib.core.models.storage.ForwardCursor;
import com.vividsolutions.jts.geom.Geometry;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A result-cursor that is not thread-safe.
 *
 * @param <T> The feature-type; if any.
 */
public class PsqlCursor<T> extends ForwardCursor<T> {

  private static final Logger log = LoggerFactory.getLogger(PsqlCursor.class);

  PsqlCursor(@NotNull PostgresSession session, @NotNull Statement stmt, @NotNull ResultSet rs) {
    this.cursor = new PostgresCursor(this, session, stmt, rs);
  }

  private PostgresCursor cursor;

  @NotNull
  PostgresCursor cur() {
    final PostgresCursor cursor = this.cursor;
    if (cursor == null || cursor.isClosed()) {
      throw new IllegalStateException("Cursor closed");
    }
    return cursor;
  }

  private final @NotNull Iterator<T> iterator = new Iterator<>() {
    @Override
    public boolean hasNext() {
      return PsqlCursor.this.hasNext();
    }

    @Override
    public T next() {
      if (!PsqlCursor.this.next()) {
        throw new NoSuchElementException();
      }
      return getFeature();
    }
  };

  @Override
  public boolean hasNext() {
    return cur().hasNext();
  }

  @Override
  public boolean next() {
    try {
      return cur().next();
    } catch (SQLException e) {
      log.atError().setMessage("Unexpected database error").setCause(e).log();
      return false;
    }
  }

  @Override
  public boolean previous() {
    try {
      return cur().previous();
    } catch (SQLException e) {
      log.atError().setMessage("Unexpected database error").setCause(e).log();
      return false;
    }
  }

  @Override
  public void beforeFirst() {
    try {
      cur().beforeFirst();
    } catch (SQLException e) {
      log.atError().setMessage("Unexpected database error").setCause(e).log();
    }
  }

  @Override
  public boolean first() {
    try {
      return cur().first();
    } catch (SQLException e) {
      log.atError().setMessage("Unexpected database error").setCause(e).log();
      return false;
    }
  }

  @Override
  public void afterLast() {
    try {
      cur().afterLast();
    } catch (SQLException e) {
      log.atError().setMessage("Unexpected database error").setCause(e).log();
    }
  }

  @Override
  public boolean last() {
    try {
      return cur().last();
    } catch (SQLException e) {
      log.atError().setMessage("Unexpected database error").setCause(e).log();
      return false;
    }
  }

  @Override
  public boolean relative(long amount) {
    try {
      return cur().relative(amount);
    } catch (SQLException e) {
      log.atError().setMessage("Unexpected database error").setCause(e).log();
      return false;
    }
  }

  @Override
  public boolean absolute(long position) {
    try {
      return cur().absolute(position);
    } catch (SQLException e) {
      log.atError().setMessage("Unexpected database error").setCause(e).log();
      return false;
    }
  }

  private <VALUE> @NotNull VALUE notNull(@Nullable VALUE value) throws NoSuchElementException {
    if (value == null) {
      throw new NoSuchElementException();
    }
    return value;
  }

  @Override
  public @NotNull EExecutedOp getOp() throws NoSuchElementException {
    try {
      return cur().op();
    } catch (SQLException e) {
      throw unchecked(e);
    }
  }

  @Override
  public @NotNull String getId() throws NoSuchElementException {
    try {
      return cur().id();
    } catch (SQLException e) {
      throw unchecked(e);
    }
  }

  @Override
  public @NotNull String getUuid() throws NoSuchElementException {
    try {
      return cur().uuid();
    } catch (SQLException e) {
      throw unchecked(e);
    }
  }

  @Override
  public @NotNull String getFeatureType() throws NoSuchElementException {
    try {
      return cur().type();
    } catch (SQLException e) {
      throw unchecked(e);
    }
  }

  @Override
  public @Nullable String getPropertiesType() throws NoSuchElementException {
    try {
      return cur().propertiesType();
    } catch (SQLException e) {
      throw unchecked(e);
    }
  }

  @Override
  public @Nullable String getJson() throws NoSuchElementException {
    try {
      return cur().rawFeature();
    } catch (SQLException e) {
      throw unchecked(e);
    }
  }

  @Override
  public byte @Nullable [] rawGeometry() throws NoSuchElementException {
    try {
      return cur().rawGeometry();
    } catch (SQLException e) {
      throw unchecked(e);
    }
  }

  @Override
  public @Nullable Geometry getGeometry() throws NoSuchElementException {
    try {
      return cur().getGeometry();
    } catch (SQLException e) {
      throw unchecked(e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public @Nullable T getFeature() throws NoSuchElementException {
    try {
      return cur().getFeature(featureClass != null ? featureClass : (Class<T>) XyzFeature.class);
    } catch (Exception e) {
      throw unchecked(e);
    }
  }

  @Override
  public <NT> @Nullable NT getFeature(@NotNull Class<NT> featureClass) throws NoSuchElementException {
    try {
      return cur().getFeature(featureClass);
    } catch (Exception e) {
      throw unchecked(e);
    }
  }

  @Override
  public void close() {
    final PostgresCursor cursor = this.cursor;
    if (cursor != null) {
      this.cursor = null;
      cursor.close();
    }
  }

  @Override
  public @NotNull Iterator<@Nullable T> iterator() {
    return iterator;
  }
}
