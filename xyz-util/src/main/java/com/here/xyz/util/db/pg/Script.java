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

package com.here.xyz.util.db.pg;

import static com.here.xyz.util.db.pg.LockHelper.buildAdvisoryLockQuery;
import static com.here.xyz.util.db.pg.LockHelper.buildAdvisoryUnlockQuery;

import com.fasterxml.jackson.core.Version;
import com.google.common.collect.Lists;
import com.here.xyz.util.Hasher;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.DatabaseSettings.ScriptResourcePath;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
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
  private String scriptIdPrefix;
  private String scriptContent;
  private DataSourceProvider dataSourceProvider;
  private String scriptVersion;
  private boolean installed;
  private Map<String, String> compatibleVersions = new HashMap<>();

  public Script(String scriptResourceLocation, DataSourceProvider dataSourceProvider, String scriptVersion) {
    this(scriptResourceLocation, dataSourceProvider, scriptVersion, null);
  }

  public Script(String scriptResourceLocation, DataSourceProvider dataSourceProvider, String scriptVersion, String scriptIdPrefix) {
    this.scriptResourceLocation = scriptResourceLocation;
    this.dataSourceProvider = dataSourceProvider;
    this.scriptVersion = scriptVersion;
    this.scriptIdPrefix = scriptIdPrefix;
  }

  private String getTargetSchema(String version) {
    String targetSchemaName = getScriptId();
    return targetSchemaName + (version != null ? ":" + version : "");
  }

  public String getScriptId() {
    String scriptName = getScriptName();
    String prefix = scriptIdPrefix == null ? "" : scriptIdPrefix + ".";
    return prefix + scriptName.substring(0, scriptName.lastIndexOf("."));
  }

  public String getScriptName() {
    return scriptResourceLocation.substring(scriptResourceLocation.lastIndexOf("/") + 1);
  }

  private String getScriptResourceFolder() {
    return scriptResourceLocation.substring(0, scriptResourceLocation.lastIndexOf("/"));
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
      logger.warn("Unable to find compatible version for script {} on DB {}. Falling back to latest version if possible.",
          getScriptName(), getDbId(), e);
      return null;
    }
  }

  public void install() {
    try {
      if (installed)
        return;
      //TODO: Remove the "anyVersionExists-check" once full qualified schemas have been installed for all scripts
      if (!getHash().equals(loadLatestHash()) || scriptVersion != null && !anyVersionExists()) {
        if (scriptVersion != null)
          install(getTargetSchema(scriptVersion), false);
        //Also install the "latest" version
        install(getTargetSchema(null), true);
        installed = true;
      }
    }
    catch (SQLException | IOException e) {
      logger.warn("Unable to install script {} on DB {}. Falling back to previous version if possible.", getScriptName(),
          getDbId(), e);
    }
  }

  private String getDbId() {
    return dataSourceProvider.getDatabaseSettings() == null ? "unknown" : dataSourceProvider.getDatabaseSettings().getId();
  }

  private void install(String targetSchema, boolean deleteBefore) throws SQLException, IOException {
    logger.info("Installing script {} on DB {} into schema {} ...", getScriptName(), getDbId(), targetSchema);

    SQLQuery scriptContent = new SQLQuery("${{scriptContent}}")
        .withQueryFragment("scriptContent", loadScriptContent());

    //Load JS-scripts to be injected
    for (Script jsScript : loadJsScripts(getScriptResourceFolder())) {
      String relativeJsScriptPath = jsScript.getScriptResourceFolder().substring(getScriptResourceFolder().length());
      scriptContent
          .withQueryFragment(relativeJsScriptPath + jsScript.getScriptName(), jsScript.loadScriptContent())
          .withQueryFragment("./" + relativeJsScriptPath + jsScript.getScriptName(), jsScript.loadScriptContent());
    }

    List<SQLQuery> installationQueries = new ArrayList<>();
    if (deleteBefore) {
      //TODO: Remove following workaround once "drop schema cascade"-bug creating orphaned functions is fixed in postgres
      installationQueries.addAll(buildDeleteFunctionQueries(loadSchemaFunctions(targetSchema)));
      installationQueries.add(buildDeleteSchemaQuery(getTargetSchema(targetSchema)));
    }
    installationQueries.addAll(List.of(buildCreateSchemaQuery(targetSchema), buildSetCurrentSearchPathQuery(targetSchema),
        buildHashFunctionQuery(), buildVersionFunctionQuery(), scriptContent));

    SQLQuery installationQuery = SQLQuery.join(installationQueries, ";")
        .withLock(targetSchema);

    //TODO: Remove the following workaround once the locking support in SQLQuery was implemented for normal update queries
    SQLQuery lockWrapper = new SQLQuery("""
        DO $lockWrapper$
        BEGIN
          ${{lockQuery}}
          ${{installationQuery}}
          ${{unlockQuery}}
        END$lockWrapper$;
        """)
        .withQueryFragment("lockQuery", buildAdvisoryLockQuery(targetSchema))
        .withQueryFragment("installationQuery", installationQuery)
        .withQueryFragment("unlockQuery", buildAdvisoryUnlockQuery(targetSchema));

    lockWrapper
        .withLoggingEnabled(false)
        .write(dataSourceProvider);
    compatibleVersions = new HashMap<>(); //Reset the cache

    logger.info("Script {} has been installed successfully on DB {} into schema {}.", getScriptName(), getDbId(), targetSchema);
  }

  private static SQLQuery buildSetCurrentSearchPathQuery(String targetSchema) {
    return new SQLQuery("SET search_path = ${currentSearchPath}, \"public\"")
        .withVariable("currentSearchPath", targetSchema);
  }

  private boolean scriptVersionExists() throws SQLException {
    return schemaExists(getTargetSchema(scriptVersion));
  }

  private boolean anyVersionExists() throws SQLException {
    return schemaExists(getTargetSchema("%"));
  }

  private String loadLatestHash() throws SQLException {
    try {
      return new SQLQuery("SELECT ${schema}.script_hash()").withVariable("schema", getTargetSchema(null))
          .run(dataSourceProvider, rs -> rs.next() ? rs.getString(1) : null);
    }
    catch (SQLException e) {
      return null;
    }
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

  private SQLQuery buildDeleteSchemaQuery(String schemaName) throws SQLException {
    return new SQLQuery("DROP SCHEMA IF EXISTS ${schema} CASCADE")
        .withVariable("schema", schemaName);
  }

  private SQLQuery buildHashFunctionQuery() throws IOException {
    //SELECT set_config(#{scriptHashVariable}, #{#scriptHash}, false);
    return new SQLQuery("""
        CREATE OR REPLACE FUNCTION script_hash() RETURNS TEXT AS
        $BODY$
        BEGIN
            RETURN '${{scriptHash}}';
        END
        $BODY$
        LANGUAGE plpgsql IMMUTABLE
        """)
        .withQueryFragment("scriptHash", getHash());
  }

  private SQLQuery buildVersionFunctionQuery() {
    return new SQLQuery("""
        CREATE OR REPLACE FUNCTION script_version() RETURNS TEXT AS
        $BODY$
        BEGIN
            RETURN '${{scriptVersion}}';
        END
        $BODY$
        LANGUAGE plpgsql IMMUTABLE
        """)
        .withQueryFragment("scriptVersion", scriptVersion);
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

  private static List<String> scanResourceFolderWA(String resourceFolder, String fileSuffix) throws IOException {
    return ((List<String>) switch (fileSuffix) {
      case ".sql" -> List.of("/sql/common.sql",  "/sql/geo.sql", "/sql/feature_writer.sql", "/jobs/transport.sql");
      case ".js" -> List.of("/sql/Exception.js", "/sql/FeatureWriter.js");
      default -> List.of();
    }).stream().filter(filePath -> filePath.startsWith(resourceFolder)).toList();
  }

  private static List<String> scanResourceFolder(ScriptResourcePath scriptResourcePath, String fileSuffix) throws IOException {
    String resourceFolder = scriptResourcePath.path();
    //TODO: Remove this workaround once the actual implementation of this method supports scanning folders inside a JAR
    if ("/sql".equals(resourceFolder) || "/jobs".equals(resourceFolder))
      return ensureInitScriptIsFirst(scanResourceFolderWA(resourceFolder, fileSuffix), scriptResourcePath.initScript());

    final InputStream folderResource = Script.class.getResourceAsStream(resourceFolder);
    if (folderResource == null)
      throw new FileNotFoundException("Resource folder " + resourceFolder + " was not found and can not be scanned for scripts.");
    BufferedReader reader = new BufferedReader(new InputStreamReader(folderResource));

    List<String> files = new ArrayList<>();
    String file;
    while ((file = reader.readLine()) != null)
      files.add(file);
    return ensureInitScriptIsFirst(files.stream()
        .filter(fileName -> fileName.endsWith(fileSuffix))
        .map(fileName -> resourceFolder + "/" + fileName)   // script.scriptResourcePath always stored as unix path
        .toList(), scriptResourcePath.initScript());
  }

  private static List<String> ensureInitScriptIsFirst(List<String> scriptPaths, String initScript) {
    if (initScript == null)
      return scriptPaths;
    String initScriptPath = scriptPaths.stream().filter(scriptPath -> scriptPath.contains(initScript)).findFirst().orElse(null);
    if (initScriptPath == null)
      return scriptPaths;

    scriptPaths = new ArrayList<>(scriptPaths);
    scriptPaths.remove(initScriptPath);
    scriptPaths.add(0, initScriptPath);
    return scriptPaths;
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
  public static List<Script> loadScripts(ScriptResourcePath scriptsResourcePath, DataSourceProvider dataSourceProvider, String scriptsVersion)
      throws IOException, URISyntaxException {
    return scanResourceFolder(scriptsResourcePath, ".sql").stream()
        .map(scriptLocation -> new Script(scriptLocation, dataSourceProvider, scriptsVersion, scriptsResourcePath.schemaPrefix()))
        .collect(Collectors.toUnmodifiableList());
  }

  private static List<Script> loadJsScripts(String scriptsResourcePath) throws IOException {
    return scanResourceFolder(new ScriptResourcePath(scriptsResourcePath), ".js").stream()
        .map(scriptLocation -> new Script(scriptLocation, null, "0.0.0"))
        .toList();
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

    //TODO: Remove following workaround once "drop schema cascade"-bug creating orphaned functions is fixed in postgres
    String schema = getTargetSchema(scriptVersion);
    batchDeleteFunctions(loadSchemaFunctions(schema));
    buildDeleteSchemaQuery(schema).write(dataSourceProvider);

    compatibleVersions = new HashMap<>(); //Reset the cache
  }

  private void batchDeleteFunctions(List<String> functionSignatures) throws SQLException {
    SQLQuery.batchOf(buildDeleteFunctionQueries(functionSignatures))
        .writeBatch(dataSourceProvider);
  }

  private static List<SQLQuery> buildDeleteFunctionQueries(List<String> functionSignatures) {
    return functionSignatures.stream()
        .map(signature -> new SQLQuery("DROP FUNCTION " + signature + " CASCADE"))
        .toList();
  }

  private List<String> loadSchemaFunctions(String schema) throws SQLException {
    return new SQLQuery("""
        SELECT proc.oid::REGPROCEDURE as signature FROM pg_proc proc LEFT JOIN pg_namespace ns ON proc.pronamespace = ns.oid
        WHERE ns.nspname = #{schema} AND proc.prokind = 'f'
        """)
        .withNamedParameter("schema", schema)
        .run(dataSourceProvider, rs -> {
          List<String> signatures = new ArrayList<>();
          while (rs.next())
            signatures.add(rs.getString("signature"));
          return signatures;
        });
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
