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

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.here.xyz.util.db.pg.LockHelper.buildAdvisoryXactLockQuery;

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
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Script {
  private static final Logger logger = LogManager.getLogger(Script.class);
  /**
   * The notice an install or uninstall statement raises, if it found its work already done under the lock.
   * It lowers client_min_messages only for its own transaction, so the notice also reaches the client if the DB setting suppresses it.
   */
  private static final String SKIPPED_NOTICE = "xyz_script_statement_skipped";
  private static final String SKIPPED_NOTICE_STATEMENT = "PERFORM set_config('client_min_messages', 'notice', true); RAISE NOTICE '"
      + SKIPPED_NOTICE + "';";
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

  /**
   * Resolves the schema to be used for the provided software version from a snapshot of the installed schemas.
   *
   * @param softwareVersion The software version or <code>null</code> for "latest"
   * @param snapshot The snapshot of the installed schemas or <code>null</code> if none is available
   * @return The schema of the newest installed version that is lower than or equal to the software version, or the unversioned
   *  schema if there is no such version, the software version is <code>null</code> or no snapshot is available
   */
  public String getCompatibleSchema(String softwareVersion, InstalledScripts snapshot) {
    if (softwareVersion == null || snapshot == null)
      return getTargetSchema(null);
    return getTargetSchema(snapshot.getNewestVersionUpTo(getScriptId(), softwareVersion));
  }

  public void install() {
    try {
      if (installed)
        return;
      if (needsInstallation(InstalledScripts.load(dataSourceProvider, List.of(this)))) {
        if (scriptVersion != null)
          installSchema(true);
        //Also install the "latest" version
        installSchema(false);
        installed = true;
      }
    }
    catch (SQLException | IOException e) {
      logger.warn("Unable to install script {} on DB {}. Falling back to previous version if possible.", getScriptName(), getDbId(), e);
    }
  }

  /**
   * Decides from a snapshot of the installed schemas whether this script has to be installed.
   *
   * @param snapshot The snapshot of the installed schemas
   * @return Whether the content of the unversioned schema differs from this script's content or the versioned schema is missing
   */
  public boolean needsInstallation(InstalledScripts snapshot) throws IOException {
    //NOTE: The versioned schema must be created even if the content did not change, because #getCompatibleSchema() resolves the
    //search path by version, not by content. Without this check a version bump without a content change leaves the new version
    //without a schema, so the resolution silently falls back to an older version which may carry outdated content.
    return !getHash().equals(snapshot.getHash(getScriptId(), null))
        || scriptVersion != null && !snapshot.exists(getScriptId(), scriptVersion);
  }

  /**
   * @param snapshot The snapshot of the installed schemas
   * @return Whether the schema this script resolves to exists: the versioned schema or, without a version, the unversioned
   *  schema with this script's content
   */
  boolean isTargetInstalled(InstalledScripts snapshot) throws IOException {
    if (scriptVersion != null)
      return snapshot.exists(getScriptId(), scriptVersion);
    return getHash().equals(snapshot.getHash(getScriptId(), null));
  }

  /**
   * Installs the versioned or the unversioned schema of this script with one statement. The statement holds an advisory lock on
   * the schema name until it commits and checks under that lock, whether the schema already has this script's content.
   *
   * @param versioned Whether to install the versioned schema. If <code>false</code>, the unversioned schema gets installed.
   * @return <code>true</code> if the schema was installed, <code>false</code> if it already had this script's content
   */
  public boolean installSchema(boolean versioned) throws SQLException, IOException {
    if (versioned && scriptVersion == null)
      throw new IllegalStateException("Script " + getScriptName() + " has no version, so it has no versioned schema.");
    String targetSchema = getTargetSchema(versioned ? scriptVersion : null);
    long start = System.currentTimeMillis();
    boolean installed = executeLocked(buildInstallationStatement(targetSchema, versioned));
    compatibleVersions = new HashMap<>(); //Reset the cache

    if (installed)
      logger.warn("Installed script {} on DB {} into schema {} in {} ms.", getScriptName(), getDbId(), targetSchema,
          System.currentTimeMillis() - start);
    else
      logger.info("Script {} was already installed on DB {} into schema {}.", getScriptName(), getDbId(), targetSchema);
    return installed;
  }

  public String getContentHash() throws IOException {
    return getHash();
  }

  String getScriptIdPrefix() {
    return scriptIdPrefix;
  }

  String getScriptVersion() {
    return scriptVersion;
  }

  private String getDbId() {
    return dataSourceProvider.getDatabaseSettings() == null ? "unknown" : dataSourceProvider.getDatabaseSettings().getId();
  }

  private SQLQuery addJsLibRegisterFunctions(SQLQuery scriptContent) throws IOException {
    String libPath = String.format("%s/lib-js", getScriptResourceFolder()),
        registerSqlFunc = """
                        CREATE OR REPLACE FUNCTION libjs_${{regFunctionName}}() RETURNS TEXT AS
                        $body$
                            SELECT regexp_replace($rfc$${{regFunctionCode}}$rfc$, '^var\\s+[^\\(]+','') AS code
                        $body$
                        LANGUAGE sql IMMUTABLE PARALLEL SAFE;
                        """;

    List<SQLQuery> queries = new ArrayList<>();
    queries.add(scriptContent);

    List<Script> jsScripts = loadJsScripts(libPath);

    for (Script jsScript : jsScripts) {
      String scriptName = jsScript.getScriptName(),
          regFunctionName = scriptName.substring(0, scriptName.indexOf('.'));

      queries.add(new SQLQuery(registerSqlFunc)
          .withQueryFragment("regFunctionName", regFunctionName)
          .withQueryFragment("regFunctionCode", jsScript.loadScriptContent()));
    }

    return SQLQuery.join(queries, "\n\n");
  }

  private SQLQuery buildInstallationStatement(String targetSchema, boolean versioned) throws IOException {
    SQLQuery scriptContent = loadSubstitutedScriptContent();
    List<SQLQuery> installationQueries = new ArrayList<>();
    //Before reinstalling the unversioned schema, all functions in it are dropped, so that functions removed from the script do not
    //linger. The drop cascades, so objects that depend on one of these functions are dropped too, also outside the schema (e.g., a
    //trigger calling one of them). The schema and its other objects are kept on purpose: the schema can hold objects the script did
    //not create (e.g., an extension registered in it, a table or a materialized view), and DROP SCHEMA ... CASCADE would drop them
    //together with every object outside the schema that depends on them. The script's own types are created only if missing, so a
    //changed type definition is not applied to this schema and needs its own migration.
    if (!versioned)
      installationQueries.add(buildDeleteFunctionsLoop(targetSchema));
    installationQueries.addAll(List.of(buildCreateSchemaQuery(targetSchema), buildSetCurrentSearchPathQuery(targetSchema),
        buildHashFunctionQuery(), buildVersionFunctionQuery(), scriptContent));

    //The installed check runs under the lock, so a concurrent installer that held the lock before is not repeated
    String installedCheck = buildFunctionBodyCheck(targetSchema, "script_hash", getHash());
    if (!versioned && scriptVersion != null)
      installedCheck += " AND " + buildFunctionBodyCheck(targetSchema, "script_version", scriptVersion);

    return new SQLQuery("""
        DO $lockWrapper$
        DECLARE _xyz_script_fn regprocedure;
        BEGIN
          ${{lockQuery}}
          IF ${{installedCheck}} THEN
            ${{skippedNotice}}
            RETURN;
          END IF;
          ${{installationQuery}}
        END$lockWrapper$;
        """)
        .withQueryFragment("lockQuery", buildAdvisoryXactLockQuery(targetSchema))
        .withQueryFragment("installedCheck", installedCheck)
        .withQueryFragment("skippedNotice", SKIPPED_NOTICE_STATEMENT)
        .withQueryFragment("installationQuery", SQLQuery.join(installationQueries, ";"));
  }

  private SQLQuery buildUninstallationStatement(String schema) {
    return new SQLQuery("""
        DO $uninstall$
        DECLARE _xyz_script_fn regprocedure;
        BEGIN
          ${{lockQuery}}
          IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = '${{schemaLiteral}}') THEN
            ${{skippedNotice}}
            RETURN;
          END IF;
          ${{deleteFunctions}};
          EXECUTE 'DROP SCHEMA ' || quote_ident('${{schemaLiteral}}') || ' CASCADE';
        END$uninstall$;
        """)
        .withQueryFragment("lockQuery", buildAdvisoryXactLockQuery(schema))
        .withQueryFragment("schemaLiteral", escapeLiteral(schema))
        .withQueryFragment("skippedNotice", SKIPPED_NOTICE_STATEMENT)
        .withQueryFragment("deleteFunctions", buildDeleteFunctionsLoop(schema));
  }

  /**
   * Builds a PL/pgSQL loop that drops all functions of the schema. It reads the functions inside the statement, so under the same
   * lock as the rest of the statement.
   */
  private static SQLQuery buildDeleteFunctionsLoop(String schema) {
    return new SQLQuery("""
        FOR _xyz_script_fn IN SELECT p.oid::regprocedure FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = '${{schemaLiteral}}' AND p.prokind = 'f' LOOP
          EXECUTE 'DROP FUNCTION ' || _xyz_script_fn::text || ' CASCADE';
        END LOOP""")
        .withQueryFragment("schemaLiteral", escapeLiteral(schema));
  }

  private static String buildFunctionBodyCheck(String schema, String functionName, String expectedValue) {
    return "EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = '" + escapeLiteral(schema)
        + "' AND p.proname = '" + functionName + "' AND p.pronargs = 0 AND substring(p.prosrc FROM 'RETURN ''([^'']*)''') = '"
        + escapeLiteral(expectedValue) + "')";
  }

  private static String escapeLiteral(String value) {
    return value.replace("'", "''");
  }

  /**
   * Executes one of the locked install or uninstall statements.
   *
   * @return <code>false</code> if the statement found its work already done under the lock, <code>true</code> otherwise
   */
  private boolean executeLocked(SQLQuery statement) throws SQLException {
    String statementText = statement.substitute().text();
    try (Connection connection = dataSourceProvider.getWriter().getConnection(); Statement stmt = connection.createStatement()) {
      stmt.execute(statementText);
      for (SQLWarning warning = stmt.getWarnings(); warning != null; warning = warning.getNextWarning())
        if (warning.getMessage() != null && warning.getMessage().contains(SKIPPED_NOTICE))
          return false;
      return true;
    }
  }

  private static SQLQuery buildSetCurrentSearchPathQuery(String targetSchema) {
    return new SQLQuery("SET search_path = ${currentSearchPath}, \"public\"")
        .withVariable("currentSearchPath", targetSchema);
  }

  public String loadLatestVersion() throws SQLException {
    return new SQLQuery("SELECT ${schema}.script_version()").withVariable("schema", getTargetSchema(null))
        .run(dataSourceProvider, rs -> rs.next() ? rs.getString(1) : null);
  }

  private SQLQuery buildCreateSchemaQuery(String schemaName) {
    return new SQLQuery("CREATE SCHEMA IF NOT EXISTS ${schema}")
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
    return Hasher.getHash(loadSubstitutedScriptContent().withLabelsEnabled(false).toExecutableQueryString());
  }

  private String readResource(String resourceLocation) throws IOException {
    InputStream is = getClass().getResourceAsStream(resourceLocation);
    if (is == null)
      throw new RuntimeException("Unable to install script " + getScriptName() + ". Resource not found: " + resourceLocation);
    try (BufferedReader buffer = new BufferedReader(new InputStreamReader(is))) {
      return buffer.lines().collect(Collectors.joining("\n"));
    }
  }

  private static List<String> scanResourceFolderWA(String resourceFolder, String fileSuffix) throws IOException {
    return ((List<String>) switch (fileSuffix) {
      case ".sql" -> List.of(
          "/sql/common.sql",
          "/sql/geo.sql",
          "/sql/feature_writer.sql",
          "/sql/h3.sql",
          "/sql/ext.sql",
          "/jobs/transport.sql"
      );
      case ".js" -> List.of(
          "/sql/Exception.js",
          "/sql/FeatureWriter.js",
          "/sql/DatabaseWriter.js",
          "/sql/lib-js/jsonpath_rfc9535.min.js"
      );
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

  private SQLQuery loadSubstitutedScriptContent() throws IOException {
    SQLQuery scriptContent = new SQLQuery("${{scriptContent}}")
        .withQueryFragment("scriptContent", loadScriptContent());

    //Load JS-scripts to be injected
    for (Script jsScript : loadJsScripts(getScriptResourceFolder())) {
      String relativeJsScriptPath = jsScript.getScriptResourceFolder().substring(getScriptResourceFolder().length());

      if (relativeJsScriptPath.startsWith("/"))
        relativeJsScriptPath = relativeJsScriptPath.substring(1);

      if (!isNullOrEmpty(relativeJsScriptPath) && !relativeJsScriptPath.endsWith("/"))
        relativeJsScriptPath = relativeJsScriptPath + "/";

      scriptContent
          .withQueryFragment(relativeJsScriptPath + jsScript.getScriptName(), jsScript.loadScriptContent())
          .withQueryFragment("./" + relativeJsScriptPath + jsScript.getScriptName(), jsScript.loadScriptContent());
    }

    if ("common.sql".equals(getScriptName())) //TODO: Use a new init-script property instead of hard-coding that one
      scriptContent = addJsLibRegisterFunctions(scriptContent);

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

  public List<String> listInstalledScriptVersions() throws SQLException {
    return InstalledScripts.load(dataSourceProvider, List.of(this)).getVersions(getScriptId());
  }

  /**
   * @param snapshot The snapshot of the installed schemas
   * @param versionsToKeep The number of the newest versions to keep
   * @return The versions older than the newest ones to keep, without the one the unversioned schema reports as its version
   */
  public List<String> versionsToUninstall(InstalledScripts snapshot, int versionsToKeep) {
    return snapshot.versionsToUninstall(getScriptId(), versionsToKeep);
  }

  /**
   * Uninstalls a versioned schema of this script with one statement, which holds an advisory lock on the schema name until it
   * commits.
   *
   * @param version The version to uninstall
   * @return <code>true</code> if the schema was uninstalled, <code>false</code> if it did not exist anymore
   */
  public boolean uninstallVersion(String version) throws SQLException {
    if (version == null)
      throw new IllegalArgumentException("Only versioned schemas can be uninstalled.");
    //TODO: Remove the function loop once "drop schema cascade"-bug creating orphaned functions is fixed in postgres
    String schema = getTargetSchema(version);
    long start = System.currentTimeMillis();
    boolean uninstalled = executeLocked(buildUninstallationStatement(schema));
    compatibleVersions = new HashMap<>(); //Reset the cache

    if (uninstalled)
      logger.warn("Uninstalled script version {}:{} on DB {} in {} ms.", getScriptName(), version, getDbId(),
          System.currentTimeMillis() - start);
    else
      logger.info("Script version {}:{} was already uninstalled on DB {}.", getScriptName(), version, getDbId());
    return uninstalled;
  }

  public void cleanupOldScriptVersions(int versionsToKeep) {
    try {
      InstalledScripts snapshot = InstalledScripts.load(dataSourceProvider, List.of(this));
      for (String scriptVersion : versionsToUninstall(snapshot, versionsToKeep)) {
        try {
          uninstallVersion(scriptVersion);
        }
        catch (Exception e) {
          logger.error("Unable to uninstall script version {}:{} on DB {} during script version cleanup.", getScriptName(),
              scriptVersion, getDbId(), e);
        }
      }
    }
    catch (Exception e) {
      logger.error("Unable to cleanup old script versions of script {} on DB {}.", getScriptName(), getDbId(), e);
    }
  }

  static Version parseVersion(String version) {
    String[] versionParts = version.split("\\.");
    String[] patchParts = versionParts[2].split("-");
    int major = Integer.parseInt(versionParts[0]);
    int minor = Integer.parseInt(versionParts[1]);
    int patch = Integer.parseInt(patchParts[0]);
    return new Version(major, minor, patch, patchParts.length == 2 ? patchParts[1] : null);
  }
}
