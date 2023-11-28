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

import com.here.naksha.lib.core.models.storage.FeatureCodec;
import com.here.naksha.lib.core.models.storage.FeatureCodecFactory;
import com.here.naksha.lib.core.models.storage.ForwardCursor;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A result-cursor that is not thread-safe.
 *
 * @param <FEATURE> The feature type that the cursor returns.
 * @param <CODEC> The codec type.
 */
public class PsqlCursor<FEATURE, CODEC extends FeatureCodec<FEATURE, CODEC>> extends ForwardCursor<FEATURE, CODEC> {

  private static final Logger log = LoggerFactory.getLogger(PsqlCursor.class);

  PsqlCursor(
      @NotNull FeatureCodecFactory<FEATURE, CODEC> codecFactory,
      @NotNull PostgresSession session,
      @NotNull Statement stmt,
      @NotNull ResultSet rs) {
    super(codecFactory);
    cursor = new PostgresCursor(this, session, stmt, rs);
  }

  private final @NotNull PostgresCursor cursor;

  @Override
  protected boolean loadNextRow(@NotNull Row row) {
    final ResultSet rs = cursor.rs;
    try {
      if (rs.next()) {
        final String r_op = rs.getString(1);
        final String r_id = rs.getString(2);
        final String r_uuid = rs.getString(3);
        final String r_type = rs.getString(4);
        final String r_ptype = rs.getString(5); // may be null
        final String r_feature = rs.getString(6); // may be null
        final byte[] r_geo = rs.getBytes(7); // may be null
        final String r_err = rs.getString(8); // may be null
        // Note: Only r_ptype, r_feature and r_geo may be null!
        assert r_op != null && r_id != null && r_uuid != null && r_type != null;

        row.codec.setOp(r_op);
        row.codec.setId(r_id);
        row.codec.setUuid(r_uuid);
        row.codec.setFeatureType(r_type);
        row.codec.setPropertiesType(r_ptype);
        row.codec.setJson(r_feature);
        row.codec.setWkb(r_geo);
        row.codec.setRawError(r_err);
        row.valid = true;
        return true;
      }
      row.clear();
      return false;
    } catch (SQLException e) {
      throw unchecked(e);
    }
  }

  @Override
  public void close() {}
}
