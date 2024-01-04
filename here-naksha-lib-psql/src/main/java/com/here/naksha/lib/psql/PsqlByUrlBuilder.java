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
package com.here.naksha.lib.psql;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.lib.core.exceptions.ParameterError;
import com.here.naksha.lib.core.models.payload.events.QueryParameterList;
import java.net.URI;
import java.net.URISyntaxException;
import org.jetbrains.annotations.NotNull;

/**
 * Base class that supports in creating builders or instances from URLs.
 */
@SuppressWarnings("unused")
abstract class PsqlByUrlBuilder<SELF extends PsqlByUrlBuilder<SELF>> {

  protected final @NotNull SELF self() {
    //noinspection unchecked
    return (SELF) this;
  }

  /**
   * Parse the given <a href="https://jdbc.postgresql.org/documentation/use/">PostgresQL URL</a> to set up the builder.
   *
   * @param postgresUrl the PostgresQL URL, for example
   *                    <br/>{@code jdbc:postgresql://{HOST}[:{PORT}]/{DB}?user={USER}&password={PASSWORD}&...={VALUE}}.
   * @return this.
   * @throws URISyntaxException If the given URL is invalid.
   * @throws ParameterError     If the given parameters are invalid.
   */
  public final @NotNull SELF parseUrl(@NotNull String postgresUrl) {
    try {
      // Syntax: jdbc:postgresql://host[:port]/db
      final URI root = new URI(postgresUrl);
      if (!"jdbc".equalsIgnoreCase(root.getScheme())) {
        throw new URISyntaxException(
            postgresUrl, "Expect scheme to be 'jdbc', but found: '" + root.getScheme() + "'");
      }
      final URI uri = new URI(root.getSchemeSpecificPart());
      if (!"postgresql".equalsIgnoreCase(uri.getScheme())) {
        throw new URISyntaxException(
            postgresUrl,
            "Expect scheme of specific part to be 'postgresql', but found: '" + uri.getScheme() + "'");
      }
      String path = uri.getPath();
      while (path != null && path.length() > 0 && path.charAt(0) == '/') {
        path = path.substring(1);
      }
      if (path == null || path.length() == 0) {
        throw new URISyntaxException(postgresUrl, "Missing database name as path");
      }
      if (path.contains("/")) {
        throw new URISyntaxException(postgresUrl, "Invalid database name: " + path);
      }

      final String host;
      if (uri.getHost() != null) {
        host = uri.getHost();
        if (host.length() == 0) {
          throw new URISyntaxException(postgresUrl, "Hostname is empty");
        }
      } else {
        throw new URISyntaxException(postgresUrl, "Hostname is empty");
      }

      final int port;
      if (uri.getPort() >= 0) {
        port = uri.getPort();
      } else {
        port = 5432;
      }
      setBasics(host, port, path);

      final String query = uri.getQuery();
      if (query != null && query.length() > 0) {
        setParams(new QueryParameterList(query));
      }
      return self();
    } catch (Throwable t) {
      throw unchecked(t);
    }
  }

  /**
   * Called by {@link #parseUrl(String)}.
   *
   * @param host The host to set.
   * @param port The port to set.
   * @param db   The database to set.
   */
  protected abstract void setBasics(@NotNull String host, int port, @NotNull String db);

  /**
   * Called by {@link #parseUrl(String)}.
   *
   * @param params The query string parameters.
   */
  protected abstract void setParams(final @NotNull QueryParameterList params);
}
