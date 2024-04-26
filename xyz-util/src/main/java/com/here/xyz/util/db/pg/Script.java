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

package com.here.xyz.util.db.pg;

import com.fasterxml.jackson.core.Version;
import com.google.common.collect.Lists;
import com.here.xyz.util.Hasher;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Script {
  private static final Logger logger = LogManager.getLogger();
  private String scriptResourceLocation;
  private String scriptContent;
  private DataSourceProvider dataSourceProvider;
  private String scriptVersion;
  private boolean installed;
  private Map<String, String> compatibleVersions = new HashMap<>();

  public Script(String scriptResourceLocation, DataSourceProvider dataSourceProvider, String scriptVersion) {
    this.scriptResourceLocation = scriptResourceLocation;
    this.dataSourceProvider = dataSourceProvider;
    this.scriptVersion = scriptVersion;
  }

  private String getTargetSchema(String version) {
    String targetSchemaPrefix = getScriptId();
    return targetSchemaPrefix + (version != null ? ":" + version : "");
  }

  public String getScriptId() {
    String scriptName = getScriptName();
    return scriptName.substring(0, scriptName.lastIndexOf("."));
  }

  public String getScriptName() {
    return scriptResourceLocation.substring(scriptResourceLocation.lastIndexOf("/") + 1);
  }

  public String getCompatibleSchema() {
    return getCompatibleSchema(scriptVersion);
  }

  public String getCompatibleSchema(String softwareVersion) {
    return getTargetSchema(getNewestScriptVersionUpto(softwareVersion));
  }

  private String getNewestScriptVersionUpto(String softwareVersion) {
    if (softwareVersion == null)
      return null;
    if (compatibleVersions.containsKey(softwareVersion))
      return compatibleVersions.get(softwareVersion);

    try {
      String compatibleVersion = null;
      for (String existingVersion : Lists.reverse(listInstalledScriptVersions())) {
        if (parseVersion(existingVersion).compareTo(parseVersion(softwareVersion)) <= 0) {
          //Stop for the first found version (NOTE: #listInstalledScriptVersions() returns a *sorted* list)
          compatibleVersion = existingVersion;
          break;
        }
      }
      compatibleVersions.put(softwareVersion, compatibleVersion);
      return compatibleVersion;
    }
    catch (SQLException e) {
      logger.error("Error finding compatible version for script {} on DB {}. Falling back to latest version if possible.",
          getScriptName(), getDbId(), e);
      return null;
    }
  }

  public void install() {
    try {
      if (!installed && (!anyVersionExists() || !scriptVersionExists() && !getHash().equals(loadLatestHash()))) {
        logger.info("Installing script {} on DB {} ...", getScriptName(), getDbId());
        if (scriptVersion != null)
          install(getTargetSchema(scriptVersion));
        //Also install the "latest" version
        deleteSchema(getTargetSchema(null));
        install(getTargetSchema(null));
        installed = true;
        logger.info("Script {} has been installed successfully on DB {}.", getScriptName(), getDbId());
      }
    }
    catch (SQLException | IOException e) {
      logger.error("Error installing script {} on DB {}. Falling back to previous version if possible.", getScriptName(),
          getDbId(), e);
    }
  }

  private String getDbId() {
    return dataSourceProvider.getDatabaseSettings() == null ? "unknown" : dataSourceProvider.getDatabaseSettings().getId();
  }

  private void install(String targetSchema) throws SQLException, IOException {
    SQLQuery setCurrentSearchPath = buildSetCurrentSearchPathQuery(targetSchema);

    SQLQuery scriptContent = new SQLQuery("${{scriptContent}}")
        .withQueryFragment("scriptContent", loadScriptContent());

    SQLQuery.batchOf(buildCreateSchemaQuery(targetSchema), setCurrentSearchPath, buildHashFunctionQuery(), buildVersionFunctionQuery(),
        scriptContent).writeBatch(dataSourceProvider);
    compatibleVersions = new HashMap<>(); //Reset the cache
  }

  private static SQLQuery buildSetCurrentSearchPathQuery(String targetSchema) {
    return new SQLQuery("SET search_path = ${currentSearchPath}")
        .withVariable("currentSearchPath", targetSchema);
  }

  private boolean scriptVersionExists() throws SQLException {
    return schemaExists(getTargetSchema(scriptVersion));
  }

  private boolean anyVersionExists() throws SQLException {
    return schemaExists(getTargetSchema("%"));
  }

  private String loadLatestHash() throws SQLException {
    return new SQLQuery("SELECT ${schema}.script_hash()").withVariable("schema", getTargetSchema(null))
        .run(dataSourceProvider, rs -> rs.next() ? rs.getString(1) : null);
  }

  public String loadLatestVersion() throws SQLException {
    return new SQLQuery("SELECT ${schema}.script_version()").withVariable("schema", getTargetSchema(null))
        .run(dataSourceProvider, rs -> rs.next() ? rs.getString(1) : null);
  }

  private boolean schemaExists(String schemaName) throws SQLException {
    return new SQLQuery("SELECT 1 FROM information_schema.schemata WHERE schema_name LIKE #{schemaName}")
        .withNamedParameter("schemaName", schemaName)
        .run(dataSourceProvider, rs -> rs.next());
  }

  private SQLQuery buildCreateSchemaQuery(String schemaName) {
    return new SQLQuery("CREATE SCHEMA IF NOT EXISTS ${schema}")
        .withVariable("schema", schemaName);
  }

  private void deleteSchema(String schemaName) throws SQLException {
    new SQLQuery("DROP SCHEMA IF EXISTS ${schema} CASCADE")
        .withVariable("schema", schemaName)
        .write(dataSourceProvider);
  }

  private SQLQuery buildHashFunctionQuery() throws IOException {
    //SELECT set_config(#{scriptHashVariable}, #{#scriptHash}, false);
    return new SQLQuery("""
        CREATE OR REPLACE FUNCTION script_hash() RETURNS TEXT AS
        $BODY$
        BEGIN
            RETURN #{scriptHash};
        END
        $BODY$
        LANGUAGE plpgsql VOLATILE;
        """)
        .withNamedParameter("scriptHash", getHash());
  }

  private SQLQuery buildVersionFunctionQuery() {
    return new SQLQuery("""
        CREATE OR REPLACE FUNCTION script_version() RETURNS TEXT AS
        $BODY$
        BEGIN
            RETURN #{scriptVersion};
        END
        $BODY$
        LANGUAGE plpgsql VOLATILE;
        """)
        .withNamedParameter("scriptVersion", scriptVersion);
  }

  private String getHash() throws IOException {
    return Hasher.getHash(loadScriptContent());
  }

  private String readResource(String resourceLocation) throws IOException {
    InputStream is = getClass().getResourceAsStream(resourceLocation);
    try (BufferedReader buffer = new BufferedReader(new InputStreamReader(is))) {
      return buffer.lines().collect(Collectors.joining("\n"));
    }
  }

  private static List<String> scanResourceFolder(String resourceFolder) throws IOException, URISyntaxException {
    List<String> files = new ArrayList<>();
    BufferedReader reader = new BufferedReader(new InputStreamReader(Script.class.getResourceAsStream(resourceFolder)));
    String file;
    while ((file = reader.readLine()) != null)
      files.add(file);
    return files.stream()
        .filter(fileName -> fileName.endsWith(".sql"))
        .map(fileName -> resourceFolder + File.separator + fileName)
        .toList();
  }

  private String loadScriptContent() throws IOException {
    if (scriptContent == null)
      scriptContent = readResource(scriptResourceLocation);
    return scriptContent;
  }

  /**
   *
   * @param scriptsResourcePath The path to the folder containing the script resources
   * @param dataSourceProvider
   * @param scriptsVersion
   * @return
   */
  public static List<Script> loadScripts(String scriptsResourcePath, DataSourceProvider dataSourceProvider, String scriptsVersion)
      throws IOException, URISyntaxException {
    return scanResourceFolder(scriptsResourcePath).stream()
        .map(scriptLocation -> new Script(scriptLocation, dataSourceProvider, scriptsVersion))
        .collect(Collectors.toUnmodifiableList());
  }

  private String extractVersion(String targetSchema) {
    int lastColonPos = targetSchema.lastIndexOf(':');
    if (lastColonPos < 0)
      return null;
    return targetSchema.substring(lastColonPos + 1);
  }

  public List<String> listInstalledScriptVersions() throws SQLException {
    List<String> targetSchemas = new SQLQuery("SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE #{scriptNameSchemaPattern}")
        .withNamedParameter("scriptNameSchemaPattern", getScriptId() + ":%")
        .run(dataSourceProvider, rs -> {
          List<String> results = new ArrayList<>();
          while (rs.next())
            results.add(rs.getString("schema_name"));
          return results;
        });

    return targetSchemas.stream()
        .map(schema -> extractVersion(schema))
        .filter(version -> version != null)
        .sorted(Comparator.comparing(Script::parseVersion))
        .toList();
  }

  private void uninstall(String scriptVersion) throws SQLException {
    if (scriptVersion.equals(loadLatestVersion()))
      throw new IllegalStateException("The script version " + getScriptName() + ":" + scriptVersion
          + " is still in use on DB " + getDbId() + " and can not be uninstalled.");

    deleteSchema(getTargetSchema(scriptVersion));
    compatibleVersions = new HashMap<>(); //Reset the cache
  }

  public void cleanupOldScriptVersions(int versionsToKeep) {
    try {
      final List<String> scriptVersions = listInstalledScriptVersions();
      if (scriptVersions.size() <= versionsToKeep)
        return;

      for (String scriptVersion : scriptVersions.subList(0, scriptVersions.size() - versionsToKeep)) {
        try {
          uninstall(scriptVersion);
        }
        catch (SQLException | IllegalStateException e) {
          logger.error("Unable to uninstall script version {}:{} on DB {} during script version cleanup.", getScriptName(),
              scriptVersion, getDbId(), e);
        }
      }
    }
    catch (SQLException e) {
      logger.error("Unable to cleanup old script versions of script {} on DB {}.", getScriptName(), getDbId(), e);
    }
  }

  private static Version parseVersion(String version) {
    String[] versionParts = version.split("\\.");
    String[] patchParts = versionParts[2].split("-");
    int major = Integer.parseInt(versionParts[0]);
    int minor = Integer.parseInt(versionParts[1]);
    int patch = Integer.parseInt(patchParts[0]);
    return new Version(major, minor, patch, patchParts.length == 2 ? patchParts[1] : null);
  }
}
