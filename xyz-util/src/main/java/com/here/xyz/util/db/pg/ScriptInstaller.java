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

import static com.here.xyz.util.db.SQLQuery.XyzSqlErrors.XYZ_FAILED_ATTEMPT;
import static com.here.xyz.util.db.pg.ScriptInstaller.State.PERMANENT;
import static com.here.xyz.util.db.pg.ScriptInstaller.State.TRANSIENT;
import static com.here.xyz.util.db.pg.ScriptInstaller.State.VERIFIED;

import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.IntSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Checks with one catalog query whether the expected script schemas are installed and installs the missing ones.
 * The decisions are the same as the ones of {@link Script#install()}, but a failing check query never leads to an installation,
 * all statements are bounded in time and run on the one provided data source provider.
 */
public class ScriptInstaller {
  private static final Logger logger = LogManager.getLogger(ScriptInstaller.class);
  /**
   * No install or uninstall statement gets started if less time than this is remaining for the invocation.
   */
  static final int MIN_REMAINING_MILLIS_FOR_DDL = 15_000;
  /**
   * The time of the invocation that the install and uninstall statements leave for the actual request.
   */
  static final int RESERVED_MILLIS_FOR_REQUEST = 10_000;
  private static final Set<String> TRANSIENT_SQL_STATES = Set.of(
      "57014", //query_canceled (e.g., statement timeout)
      "55P03", //lock_not_available (lock timeout)
      "57P01", //admin_shutdown
      "57P02", //crash_shutdown
      "57P03", //cannot_connect_now
      "53300", //too_many_connections
      "40001", //serialization_failure
      "40P01", //deadlock_detected
      "23505", //unique_violation, e.g., a concurrent installer which released its lock before committing created the schema
      "25006" //read_only_sql_transaction, e.g., the writer endpoint still points to a read-only instance during a failover
  );

  public enum State {
    /**
     * The schemas of all scripts for the software version exist.
     */
    VERIFIED,
    /**
     * The schema of at least one script is missing and its installation failed with an error that is not expected to go away.
     */
    PERMANENT,
    /**
     * The check or an installation failed with an error that may go away (e.g., a connection error or a timeout).
     */
    TRANSIENT
  }

  /**
   * @param state The resulting state
   * @param schemas The schema for each script to be used in the search path, in the order of the scripts
   * @param statements The number of statements that were executed
   * @param snapshot The latest snapshot of the installed schemas, <code>null</code> if no snapshot could be loaded
   */
  public record Outcome(State state, List<String> schemas, int statements, InstalledScripts snapshot) {}

  private ScriptInstaller() {}

  /**
   * Checks whether the schemas of the provided scripts are installed, installs the missing ones and cleans up old versions.
   *
   * @param scripts The scripts in the order of their installation. They must use the provided data source provider.
   * @param dataSourceProvider The data source provider to be used for all statements
   * @param softwareVersion The software version or <code>null</code> for "latest"
   * @param versionsToKeep The number of the newest versions to keep per script
   * @param installTimeoutSeconds The statement and lock timeout for each install and uninstall statement
   * @param remainingMillis Provides the remaining time of the invocation, <code>null</code> if there is no limit
   * @return The outcome of the check
   */
  public static Outcome checkAndInstall(List<Script> scripts, DataSourceProvider dataSourceProvider, String softwareVersion,
      int versionsToKeep, int installTimeoutSeconds, IntSupplier remainingMillis) {
    return new Check(scripts, dataSourceProvider, softwareVersion, versionsToKeep, installTimeoutSeconds, remainingMillis).run();
  }

  /**
   * @param scripts The scripts
   * @param softwareVersion The software version or <code>null</code> for "latest"
   * @param snapshot A snapshot of the installed schemas or <code>null</code> if none is available
   * @return For each script, the newest installed version that is lower than or equal to the software version, or the unversioned
   *  schema if there is none
   */
  public static List<String> resolveSchemas(List<Script> scripts, String softwareVersion, InstalledScripts snapshot) {
    return scripts.stream().map(script -> script.getCompatibleSchema(softwareVersion, snapshot)).toList();
  }

  static boolean isTransient(SQLException e) {
    Throwable t = e;
    for (int depth = 0; t != null && depth < 10; depth++) {
      if (t instanceof SQLTimeoutException)
        return true;
      if (t instanceof SQLException sqlException) {
        String sqlState = sqlException.getSQLState();
        if (sqlState != null && (sqlState.startsWith("08") || TRANSIENT_SQL_STATES.contains(sqlState)
            || XYZ_FAILED_ATTEMPT.errorCode.equals(sqlState)))
          return true;
        //NOTE: SQLQuery's FailedExecutionAttempt can not be referenced from here and its #getCause() is not safe to call
        if ("FailedExecutionAttempt".equals(sqlException.getClass().getSimpleName()))
          return true;
        if (sqlException.getMessage() != null && sqlException.getMessage().contains("tuple concurrently updated"))
          return true;
      }
      if (t.getCause() == t)
        break;
      t = t.getCause();
    }
    return false;
  }

  private static class Check {
    private final List<Script> scripts;
    private final DataSourceProvider dataSourceProvider;
    private final String softwareVersion;
    private final int versionsToKeep;
    private final int installTimeoutSeconds;
    private final IntSupplier remainingMillis;
    private final String dbId;
    private final long start = System.currentTimeMillis();
    private int statements;
    private long ddlTimeoutMillis = -1;
    private boolean ddlExecuted;
    /**
     * The scripts whose target schema (the versioned one if there is a version, the unversioned one otherwise) failed to install
     * with a permanent error
     */
    private final Set<Script> permanentlyFailedTargets = new HashSet<>();

    private Check(List<Script> scripts, DataSourceProvider dataSourceProvider, String softwareVersion, int versionsToKeep,
        int installTimeoutSeconds, IntSupplier remainingMillis) {
      this.scripts = scripts;
      this.dataSourceProvider = dataSourceProvider;
      this.softwareVersion = softwareVersion;
      this.versionsToKeep = versionsToKeep;
      this.installTimeoutSeconds = Math.max(1, installTimeoutSeconds);
      this.remainingMillis = remainingMillis;
      dbId = dataSourceProvider.getDatabaseSettings() == null ? "unknown" : dataSourceProvider.getDatabaseSettings().getId();
    }

    private Outcome run() {
      InstalledScripts snapshot = loadSnapshot();
      if (snapshot == null)
        //A failing check never leads to an installation
        return finish(TRANSIENT, null);

      installMissingSchemas(snapshot);

      InstalledScripts finalSnapshot = snapshot;
      if (ddlExecuted) {
        //The re-check must run with the timeout of the check, not with the one of the install statements
        if (!resetDdlTimeouts())
          return finish(TRANSIENT, snapshot);
        finalSnapshot = loadSnapshot();
        if (finalSnapshot == null)
          return finish(TRANSIENT, snapshot);
      }

      List<Script> missingTargets = missingTargets(finalSnapshot);
      if (missingTargets.isEmpty())
        return finish(VERIFIED, cleanup(finalSnapshot));
      //Only a permanent error on a missing target itself is permanent. E.g., an error while rewriting the unversioned schema of a
      //script whose versioned schema exists must not keep another script's transient miss for the lifetime of the JVM.
      return finish(missingTargets.stream().anyMatch(permanentlyFailedTargets::contains) ? PERMANENT : TRANSIENT, finalSnapshot);
    }

    private InstalledScripts loadSnapshot() {
      try {
        InstalledScripts snapshot = InstalledScripts.load(dataSourceProvider, scripts);
        statements += snapshot.getStatementCount();
        return snapshot;
      }
      catch (SQLException | RuntimeException e) {
        statements++;
        logger.warn("Unable to load the installed scripts on DB {}.", dbId, e);
        return null;
      }
    }

    private void installMissingSchemas(InstalledScripts snapshot) {
      for (Script script : scripts) {
        boolean needsInstallation;
        try {
          needsInstallation = script.needsInstallation(snapshot);
        }
        catch (IOException | RuntimeException e) {
          logger.error("Unable to read script {} for the installation on DB {}.", script.getScriptName(), dbId, e);
          permanentlyFailedTargets.add(script);
          continue;
        }
        if (!needsInstallation)
          continue;

        //Install the versioned schema first, then the unversioned one. If the versioned one fails, skip the unversioned one.
        boolean versionedFailed = false;
        if (script.getScriptVersion() != null) {
          if (!hasTimeForDdl())
            return;
          versionedFailed = !installSchema(script, true);
        }
        if (!versionedFailed) {
          if (!hasTimeForDdl())
            return;
          installSchema(script, false);
        }
      }
    }

    private boolean hasTimeForDdl() {
      if (remainingMillis() >= MIN_REMAINING_MILLIS_FOR_DDL)
        return true;
      logger.warn("Not enough time left to install scripts on DB {}.", dbId);
      return false;
    }

    private int remainingMillis() {
      return remainingMillis == null ? Integer.MAX_VALUE : remainingMillis.getAsInt();
    }

    /**
     * Bounds the following install and uninstall statements by the install timeout and by the time that is remaining for the
     * invocation minus the time reserved for the request.
     */
    private void applyDdlTimeouts() throws SQLException {
      long timeoutMillis = Math.min(installTimeoutSeconds * 1_000L, (long) remainingMillis() - RESERVED_MILLIS_FOR_REQUEST);
      if (timeoutMillis == ddlTimeoutMillis)
        return;
      statements++;
      new SQLQuery("SET statement_timeout = " + timeoutMillis + "; SET lock_timeout = " + timeoutMillis)
          .withLoggingEnabled(false)
          .write(dataSourceProvider);
      ddlTimeoutMillis = timeoutMillis;
    }

    /**
     * Restores the statement timeout of the check and removes the lock timeout, which both were set for the install statements.
     *
     * @return Whether the timeouts could be reset
     */
    private boolean resetDdlTimeouts() {
      if (ddlTimeoutMillis < 0)
        return true;
      try {
        statements++;
        //NOTE: RESET restores the value of the connection's startup options
        new SQLQuery("RESET statement_timeout; RESET lock_timeout")
            .withLoggingEnabled(false)
            .write(dataSourceProvider);
        ddlTimeoutMillis = -1;
        return true;
      }
      catch (SQLException e) {
        logger.warn("Unable to reset the timeouts after installing scripts on DB {}.", dbId, e);
        return false;
      }
    }

    /**
     * @return Whether the installation succeeded
     */
    private boolean installSchema(Script script, boolean versioned) {
      String schema = versioned ? script.getScriptId() + ":" + script.getScriptVersion() : script.getScriptId();
      //The target is the schema the script resolves to: the versioned one if there is a version, the unversioned one otherwise
      boolean target = versioned == (script.getScriptVersion() != null);
      try {
        applyDdlTimeouts();
        statements++;
        ddlExecuted = true;
        script.installSchema(versioned);
        return true;
      }
      catch (SQLException e) {
        boolean transientError = isTransient(e);
        if (!transientError && target)
          permanentlyFailedTargets.add(script);
        logger.warn("Unable to install script {} into schema {} on DB {} ({} error).", script.getScriptName(), schema, dbId,
            transientError ? "transient" : "permanent", e);
        return false;
      }
      catch (IOException | RuntimeException e) {
        if (target)
          permanentlyFailedTargets.add(script);
        logger.warn("Unable to install script {} into schema {} on DB {} (permanent error).", script.getScriptName(), schema, dbId, e);
        return false;
      }
    }

    /**
     * @return The scripts whose target schema is not installed according to the snapshot
     */
    private List<Script> missingTargets(InstalledScripts snapshot) {
      List<Script> missingTargets = new ArrayList<>();
      for (Script script : scripts) {
        try {
          if (!script.isTargetInstalled(snapshot))
            missingTargets.add(script);
        }
        catch (IOException | RuntimeException e) {
          missingTargets.add(script);
        }
      }
      return missingTargets;
    }

    /**
     * Uninstalls the old script versions. Errors are logged, but do not change the outcome of the check.
     *
     * @return The snapshot without the uninstalled versions
     */
    private InstalledScripts cleanup(InstalledScripts snapshot) {
      InstalledScripts result = snapshot;
      for (Script script : scripts) {
        for (String version : script.versionsToUninstall(snapshot, versionsToKeep)) {
          if (remainingMillis() < MIN_REMAINING_MILLIS_FOR_DDL) {
            logger.info("Not enough time left to clean up old script versions on DB {}.", dbId);
            return result;
          }
          try {
            applyDdlTimeouts();
            statements++;
            script.uninstallVersion(version);
            result = result.withoutVersion(script.getScriptId(), version);
          }
          catch (Exception e) {
            logger.error("Unable to uninstall script version {}:{} on DB {} during script version cleanup.", script.getScriptName(),
                version, dbId, e);
          }
        }
      }
      return result;
    }

    private Outcome finish(State state, InstalledScripts snapshot) {
      List<String> schemas = resolveSchemas(scripts, softwareVersion, snapshot);
      long duration = System.currentTimeMillis() - start;
      if (state == VERIFIED)
        logger.info("Script check on DB {} for version {}: {} in {} ms with {} statements, schemas: {}", dbId, softwareVersion, state,
            duration, statements, schemas);
      else
        logger.warn("Script check on DB {} for version {}: {} in {} ms with {} statements, schemas: {}", dbId, softwareVersion, state,
            duration, statements, schemas);
      return new Outcome(state, schemas, statements, snapshot);
    }
  }
}
