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

package com.here.xyz.util.db.pg;

import com.fasterxml.jackson.core.Version;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A snapshot of the installed script schemas, read from the database catalog with one query.
 * Every versioned schema {@code <scriptId>:<version>} is created by one statement, so it exists only when it is complete. Its
 * {@code script_hash()} and {@code script_version()} functions carry the hash and the version as literals in their bodies.
 */
public class InstalledScripts {
  private static final String BODY_LITERAL = "substring(p.prosrc FROM 'RETURN ''([^'']*)''')";
  private final Map<String, ScriptSchemas> scripts = new HashMap<>();
  private final int statementCount;

  record Row(String schemaName, boolean hasHashFunction, String hash, boolean hasVersionFunction, String version) {}

  private static class ScriptSchemas {
    private final List<String> versions = new ArrayList<>();
    private final Map<String, String> hashes = new HashMap<>();
    private boolean unversionedExists;
    private String unversionedHash;
    private String unversionedVersion;

    private ScriptSchemas copy() {
      ScriptSchemas copy = new ScriptSchemas();
      copy.versions.addAll(versions);
      copy.hashes.putAll(hashes);
      copy.unversionedExists = unversionedExists;
      copy.unversionedHash = unversionedHash;
      copy.unversionedVersion = unversionedVersion;
      return copy;
    }
  }

  InstalledScripts(Collection<String> scriptIds, List<Row> rows) {
    this(scriptIds, rows, 0);
  }

  private InstalledScripts(Collection<String> scriptIds, List<Row> rows, int statementCount) {
    this.statementCount = statementCount;
    scriptIds.forEach(scriptId -> scripts.put(scriptId, new ScriptSchemas()));
    for (Row row : rows) {
      int colonPos = row.schemaName().indexOf(':');
      String scriptId = colonPos < 0 ? row.schemaName() : row.schemaName().substring(0, colonPos);
      ScriptSchemas schemas = scripts.get(scriptId);
      if (schemas == null)
        continue;

      if (colonPos < 0) {
        schemas.unversionedExists = true;
        schemas.unversionedHash = row.hash();
        schemas.unversionedVersion = row.version();
      }
      else {
        String version = row.schemaName().substring(colonPos + 1);
        if (parse(version) == null)
          continue;
        schemas.versions.add(version);
        schemas.hashes.put(version, row.hash());
      }
    }
    scripts.values().forEach(schemas -> schemas.versions.sort(Comparator.comparing(InstalledScripts::parse)));
  }

  private InstalledScripts(InstalledScripts other) {
    statementCount = other.statementCount;
    other.scripts.forEach((scriptId, schemas) -> scripts.put(scriptId, schemas.copy()));
  }

  /**
   * Reads all installed schemas of the provided scripts with one catalog query on the writer of the provided data source provider.
   * Only if a {@code script_hash()} or {@code script_version()} function exists, but its body could not be parsed, that function
   * gets called additionally.
   *
   * @param dataSourceProvider The data source provider to be used
   * @param scripts The scripts for which to load the installed schemas
   * @return The snapshot of the installed schemas
   * @throws SQLException If one of the queries failed
   */
  public static InstalledScripts load(DataSourceProvider dataSourceProvider, List<Script> scripts) throws SQLException {
    if (scripts.isEmpty())
      return new InstalledScripts(List.of(), List.of());

    Set<String> scriptIds = new LinkedHashSet<>();
    Set<String> patterns = new LinkedHashSet<>();
    for (Script script : scripts) {
      scriptIds.add(script.getScriptId());
      patterns.add(script.getScriptIdPrefix() != null ? escapeLikePattern(script.getScriptIdPrefix() + ".") + "%"
          : escapeLikePattern(script.getScriptId()) + "%");
    }

    List<Row> rows = buildSnapshotQuery(new ArrayList<>(patterns)).run(dataSourceProvider, rs -> {
      List<Row> result = new ArrayList<>();
      while (rs.next())
        result.add(new Row(rs.getString("schema_name"), rs.getBoolean("has_hash"), rs.getString("hash"),
            rs.getBoolean("has_version"), rs.getString("version")));
      return result;
    });

    int statementCount = 1;
    List<Row> resolvedRows = new ArrayList<>();
    for (Row row : rows) {
      String hash = row.hash();
      String version = row.version();
      if (row.hasHashFunction() && hash == null) {
        hash = callFunction(dataSourceProvider, row.schemaName(), "script_hash");
        statementCount++;
      }
      if (row.hasVersionFunction() && version == null) {
        version = callFunction(dataSourceProvider, row.schemaName(), "script_version");
        statementCount++;
      }
      resolvedRows.add(new Row(row.schemaName(), row.hasHashFunction(), hash, row.hasVersionFunction(), version));
    }
    return new InstalledScripts(scriptIds, resolvedRows, statementCount);
  }

  private static SQLQuery buildSnapshotQuery(List<String> patterns) {
    List<String> conditions = new ArrayList<>();
    Map<String, Object> parameters = new HashMap<>();
    for (int i = 0; i < patterns.size(); i++) {
      conditions.add("n.nspname LIKE #{p" + i + "}");
      parameters.put("p" + i, patterns.get(i));
    }
    //NOTE: The privilege filter is the one of information_schema.schemata, so the visibility stays the same as before
    return new SQLQuery("""
        SELECT n.nspname AS schema_name,
          EXISTS (SELECT 1 FROM pg_proc p WHERE p.pronamespace = n.oid AND p.proname = 'script_hash' AND p.pronargs = 0) AS has_hash,
          (SELECT ${{bodyLiteral}} FROM pg_proc p
            WHERE p.pronamespace = n.oid AND p.proname = 'script_hash' AND p.pronargs = 0 LIMIT 1) AS hash,
          EXISTS (SELECT 1 FROM pg_proc p WHERE p.pronamespace = n.oid AND p.proname = 'script_version' AND p.pronargs = 0) AS has_version,
          (SELECT ${{bodyLiteral}} FROM pg_proc p
            WHERE p.pronamespace = n.oid AND p.proname = 'script_version' AND p.pronargs = 0 LIMIT 1) AS version
        FROM pg_namespace n
        WHERE (${{conditions}}) AND (pg_has_role(n.nspowner, 'USAGE') OR has_schema_privilege(n.oid, 'CREATE, USAGE'))
        """)
        .withQueryFragment("bodyLiteral", BODY_LITERAL)
        .withQueryFragment("conditions", String.join(" OR ", conditions))
        .withNamedParameters(parameters);
  }

  private static String callFunction(DataSourceProvider dataSourceProvider, String schema, String function) throws SQLException {
    return new SQLQuery("SELECT ${schema}." + function + "()")
        .withVariable("schema", schema)
        .run(dataSourceProvider, rs -> rs.next() ? rs.getString(1) : null);
  }

  private static String escapeLikePattern(String value) {
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
  }

  private static Version parse(String version) {
    try {
      return Script.parseVersion(version);
    }
    catch (RuntimeException e) {
      return null;
    }
  }

  private ScriptSchemas schemas(String scriptId) {
    ScriptSchemas schemas = scripts.get(scriptId);
    return schemas == null ? new ScriptSchemas() : schemas;
  }

  int getStatementCount() {
    return statementCount;
  }

  /**
   * @param scriptId The script ID
   * @return The installed versions of the script, sorted from the oldest to the newest
   */
  public List<String> getVersions(String scriptId) {
    return List.copyOf(schemas(scriptId).versions);
  }

  /**
   * @param scriptId The script ID
   * @param version The version or <code>null</code> for the unversioned schema
   * @return Whether the schema exists
   */
  public boolean exists(String scriptId, String version) {
    return version == null ? schemas(scriptId).unversionedExists : schemas(scriptId).hashes.containsKey(version);
  }

  /**
   * @param scriptId The script ID
   * @param version The version or <code>null</code> for the unversioned schema
   * @return The value of the schema's {@code script_hash()} or <code>null</code> if the schema or the function does not exist
   */
  public String getHash(String scriptId, String version) {
    return version == null ? schemas(scriptId).unversionedHash : schemas(scriptId).hashes.get(version);
  }

  /**
   * @param scriptId The script ID
   * @return The value of the unversioned schema's {@code script_version()} or <code>null</code> if it does not exist
   */
  public String getUnversionedVersion(String scriptId) {
    return schemas(scriptId).unversionedVersion;
  }

  /**
   * @param scriptId The script ID
   * @param softwareVersion The software version
   * @return The newest installed version that is lower than or equal to the software version or <code>null</code> if there is none
   */
  public String getNewestVersionUpTo(String scriptId, String softwareVersion) {
    Version maxVersion = softwareVersion == null ? null : parse(softwareVersion);
    if (maxVersion == null)
      return null;
    List<String> versions = schemas(scriptId).versions;
    for (int i = versions.size() - 1; i >= 0; i--)
      if (parse(versions.get(i)).compareTo(maxVersion) <= 0)
        return versions.get(i);
    return null;
  }

  /**
   * @param scriptId The script ID
   * @param versionsToKeep The number of the newest versions to keep
   * @return The versions older than the newest ones to keep, without the one the unversioned schema reports as its version
   */
  public List<String> versionsToUninstall(String scriptId, int versionsToKeep) {
    List<String> versions = schemas(scriptId).versions;
    if (versions.size() <= versionsToKeep)
      return List.of();
    String versionInUse = getUnversionedVersion(scriptId);
    return versions.subList(0, versions.size() - versionsToKeep).stream()
        .filter(version -> !version.equals(versionInUse))
        .toList();
  }

  InstalledScripts withoutVersion(String scriptId, String version) {
    InstalledScripts copy = new InstalledScripts(this);
    ScriptSchemas schemas = copy.scripts.get(scriptId);
    if (schemas != null) {
      schemas.versions.remove(version);
      schemas.hashes.remove(version);
    }
    return copy;
  }
}
