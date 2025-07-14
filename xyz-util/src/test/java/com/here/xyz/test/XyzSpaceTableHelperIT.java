/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.test.featurewriter.SpaceWriter;
import com.here.xyz.test.featurewriter.sql.SQLSpaceWriter;
import com.here.xyz.util.db.ConnectorParameters.TableLayout;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper.TableComment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.here.xyz.util.Random.randomAlpha;

public class XyzSpaceTableHelperIT extends SQLITBase {
    private final String SCHEMA = "public";
    private final String TABLE = getClass().getSimpleName() + "_" + randomAlpha(5);

    protected SpaceWriter spaceWriter = new SQLSpaceWriter(false, TABLE);

    @BeforeEach
    public void setup() throws Exception {
        spaceWriter.createSpaceResources();
    }

    @AfterEach
    public void tearDown() throws Exception {
        spaceWriter.cleanSpaceResources();
    }

    @Test
    public void createTableComment() throws Exception {
      TableLayout v2 = TableLayout.V2;

      try (DataSourceProvider dsp = getDataSourceProvider()) {
          SQLQuery addTableComment = XyzSpaceTableHelper.buildAddTableCommentQuery(SCHEMA, TABLE, new TableComment(TABLE, v2));
          addTableComment.write(dsp);

          SQLQuery readTableComment = XyzSpaceTableHelper.buildReadTableCommentQuery(SCHEMA, TABLE);
          readTableComment.run(dsp, rs -> {
              if (rs.next()) {
                  TableComment comment;
                  try {
                      comment = XyzSerializable.deserialize(rs.getString("comment"), TableComment.class);
                      Assertions.assertEquals(TableLayout.V2, comment.tableLayout());
                      Assertions.assertEquals(TABLE, comment.spaceId());
                  } catch (JsonProcessingException e) {
                      throw new RuntimeException(e);
                  }
              } else {
                  Assertions.fail("Table comment not found for " + TABLE);
              }
              return null;
          });
      }
    }
}
