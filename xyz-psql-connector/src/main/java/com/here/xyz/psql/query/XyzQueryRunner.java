/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

package com.here.xyz.psql.query;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.Event;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.responses.XyzResponse;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This class provides the utility to run a single database query which is described by an XYZ {@link Event}
 * and which returns an {@link XyzResponse}.
 * It has the internal capability to build & run the necessary {@link SQLQuery} and translate
 * the resulting {@link ResultSet} into an {@link XyzResponse}.
 * @param <E> The event type
 * @param <R> The response type
 */
public abstract class XyzQueryRunner<E extends Event, R extends XyzResponse> extends XyzEventBasedQueryRunner<E, R> {

  public XyzQueryRunner(E event)
      throws SQLException, ErrorResponseException {
    super(event);
  }

}
