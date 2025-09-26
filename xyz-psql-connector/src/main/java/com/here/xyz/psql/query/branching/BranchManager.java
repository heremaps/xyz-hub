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

package com.here.xyz.psql.query.branching;

import static com.here.xyz.models.hub.Branch.MAIN_BRANCH;
import static com.here.xyz.models.hub.Ref.HEAD;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildCreateBranchTableQueries;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.sequenceName;

import com.google.common.collect.Lists;
import com.here.xyz.events.UpdateStrategy.OnMergeConflict;
import com.here.xyz.events.UpdateStrategy.OnVersionConflict;
import com.here.xyz.events.WriteFeaturesEvent.Modification;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.psql.query.branching.CommitManager.SimpleCommitResult;
import com.here.xyz.psql.query.branching.HistoryManager.ChangesetIterator;
import com.here.xyz.responses.changesets.Changeset;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BranchManager {
  protected final DataSourceProvider dataSourceProvider;
  final String streamId;
  final String spaceId;
  final String schema;
  final String rootTable;
  final CommitManager commitManager;
  final HistoryManager historyManager;

  public BranchManager(DataSourceProvider dataSourceProvider, String streamId, String spaceId, String schema, String rootTable) {
    this.dataSourceProvider = dataSourceProvider;
    this.streamId = streamId;
    this.spaceId = spaceId;
    this.schema = schema;
    this.rootTable = rootTable;
    this.commitManager = new CommitManager(this);
    this.historyManager = new HistoryManager(this);
  }

  /**
   * Creates a new branch on the specified branching-point
   * @param baseRef The branching point. That depicts the base branch and the base version of the newly created branch.
   * @return The current (resolved) HEAD-ref of the newly created branch
   * @throws SQLException
   */
  public Ref createBranch(Ref baseRef) throws SQLException {
    if (baseRef.isHead())
      baseRef = resolveHead(baseRef);
    else {
      Ref currentHeadRef = resolveHead(Ref.fromBranchId(baseRef.getBranch()));
      if (baseRef.getVersion() > currentHeadRef.getVersion())
        throw new IllegalArgumentException("Branch could not be created: The provided base ref points to a version that is larger than the "
            + "current HEAD of the base branch: HEAD=" + currentHeadRef.getVersion());
      //TODO: Also check if baseRef.getVersion() == 0 and "simplify" the baseRef to the previous level then - throw an exception if (finally) the baseRef is: ~0:0, because no branch may be created of that one
    }
    int newNodeId = getNewNodeId();
    SQLQuery.batchOf(buildCreateBranchTableQueries(schema, branchTableName(getNodeId(baseRef), baseRef.getVersion(), newNodeId), spaceId))
        .writeBatch(dataSourceProvider);

    long newHeadVersion = baseRef.getVersion();
    return Ref.fromBranchId("~" + newNodeId, newHeadVersion);
  }

  public void writeCommit(int nodeId, Set<Modification> modifications, String author) throws SQLException {
    writeCommit(nodeId, modifications, author, new Ref(HEAD));
  }

  public SimpleCommitResult writeCommit(int nodeId, Set<Modification> modifications, String author, Ref baseRef) throws SQLException {
    return writeCommit(nodeId, modifications, author, baseRef, -1);
  }

  public SimpleCommitResult writeCommit(int nodeId, Set<Modification> modifications, String author, Ref baseRef, long version) throws SQLException {
    return commitManager.writeCommit(nodeId, modifications, author, baseRef, version);
  }

  //TODO: Ensure the branch / node is not writable for other processes during a rebase
  public BranchOperationResult rebase(int nodeId, Ref baseRef, Ref newBaseRef) throws SQLException {
    //TODO: Do this inside an environment that is capable of running longer (e.g., async postgres query? / job? / longer running lambda?)
    //Create a new branch that is based on the new branching point
    int newNodeId = getNodeId(createBranch(newBaseRef));
    newBaseRef = resolveHead(newBaseRef);

    Ref baseRefInCommonAncestor = findBaseRefInCommonAncestor(baseRef, newBaseRef);
    ChangesetIterator changesetIterator = historyManager.iterateChangesets(baseRefInCommonAncestor, headRef(nodeId));

    List<SimpleCommitResult> commitResults = new ArrayList<>();
    long newVersion = -1;
    //Add all commits of the branch (since the branching point in the common ancestor) to the new branch while using the baseRef in the common ancestor as the baseRef of the commit-procedure
    while (changesetIterator.hasNext()) {
      //NOTE: Due to pagination, each returned changeset object might be partial. So there could be multiple changeset objects with the same version after each other (distributed across multiple pages)
      Changeset changeset = changesetIterator.next();
      newVersion = changeset.getVersion() - baseRef.getVersion() + newBaseRef.getVersion();
      //NOTE: Using OnMergeConflict.CONTINUE here to ensure to continue writing a conflicting feature (while marking it as such) and continue writing all following features as well
      //TODO: Ensure to keep old timestamps (for all non-conflicting writes)
      SimpleCommitResult commitResult = writeCommit(newNodeId, changeset.toModifications(OnVersionConflict.MERGE, OnMergeConflict.CONTINUE), changeset.getAuthor(), baseRefInCommonAncestor, newVersion);
      commitResults.add(commitResult);
    }
    if (newVersion != -1)
      setNextVersion(newNodeId, newBaseRef, newVersion - newBaseRef.getVersion());
    //NOTE: Returning & using the new node ID from now on, because for empty rebased branches that actually makes a difference for their new base-ref

    //TODO: Do the following only on success
    //deleteBranch(nodeId); //TODO: Re-activate later, for now the original branches will be deleted by the prune-process

    //TODO: Add some "continueRebase()" method to be used to solve conflicts (could be called multiple times until the full rebase succeeds)

    return new BranchOpResult(newNodeId, newBaseRef,
        commitResults.stream().mapToInt(commitResult -> commitResult.conflicting()).sum() > 0);
  }

  public MergeOperationResult merge(int nodeId, Ref baseRef, int targetNodeId, boolean fastForward) throws SQLException {
    //TODO: Also implement fastForward
    if (fastForward)
      throw new UnsupportedOperationException("fast-forward merge is not implemented yet");

    /*
    Non-fast-forward merge implementation:
    - Create tmp branch with base: <targetNodeId>:HEAD
    - Write all changes from the source branch (squashed) to into the tmp branch with baseRef: commonBaseRef(baseRef, targetNodeId)
    - If there are conflicts, stop and return the nodeId of the tmp branch
    - If there are no conflicts, write the (one) changeset of the tmp branch into the target branch with baseRef: <targetNodeId>:HEAD
      - delete the tmp branch
      - return the nodeId of the target branch to indicate that the merge was completed without conflicts
     */

    Ref branchHeadRef = resolveHead(headRef(nodeId));

    //Create a temporary branch that is based on the HEAD of the target branch into which to merge (ensure that the targetRef is resolved, because there could be even incoming changes into the target branch during the merge process)
    Ref targetRef = resolveHead(headRef(targetNodeId));
    Ref tmpRef = createBranch(targetRef);
    int tmpNodeId = getNodeId(tmpRef);

    Ref mergeCommitRef = Ref.fromBranchId(targetRef.getBranch(), targetRef.getVersion() + 1);
    Ref baseRefInCommonAncestor = findBaseRefInCommonAncestor(baseRef, targetRef);
    ChangesetIterator changesetIterator = historyManager.iterateChangesets(baseRefInCommonAncestor, branchHeadRef);

    List<SimpleCommitResult> commitResults = new ArrayList<>();
    //Add all commits of the branch (since the branching point in the common ancestor) to the tmp branch while using the baseRef in the common ancestor as the baseRef of the commit-procedure
    while (changesetIterator.hasNext()) {
      //NOTE: Due to pagination, each returned changeset object might be partial. So there could be multiple changeset objects with the same version after each other (distributed across multiple pages)
      Changeset changeset = changesetIterator.next();
      //Take the same version for all writes (squashing), that is the HEAD-version of the tmp-branch + 1
      long newVersion = mergeCommitRef.getVersion();
      //NOTE: Using OnMergeConflict.CONTINUE here to ensure to continue writing a conflicting feature (while marking it as such) and continue writing all following features as well
      //TODO: Ensure to keep the timestamps of the newest commit of the squashed commits
      SimpleCommitResult result = writeCommit(tmpNodeId, changeset.toModifications(OnVersionConflict.MERGE, OnMergeConflict.CONTINUE), changeset.getAuthor(), baseRefInCommonAncestor, newVersion);
      commitResults.add(result);
    }
    if (commitResults.isEmpty())
      //Nothing to do, the branch had no changes. Nothing to be merged.
      return finalizeMerge(branchHeadRef, baseRef, tmpNodeId, mergeCommitRef);
    else
      //Increase the sequence of the target branch
      getNextVersion(tmpNodeId);
    if (commitResults.stream().mapToInt(commitResult -> commitResult.conflicting()).sum() > 0)
      //There are conflicts, pause the merge and return the conflicting branch's node ID & baseRef
      return new MergeOperationResult(tmpNodeId, targetRef, true, branchHeadRef.getVersion(), mergeCommitRef);

    //Increase the sequence of the target branch
    getNextVersion(targetNodeId);
    //TODO: Check here again, if the resolved HEAD of the target was not changing since the beginning of the merge process (it should be equal to mergeCommitRef) - otherwise try full merge from scratch?
    //Write the one commit from the tmp branch as new commit into the target branch
    ChangesetIterator tmpChangesetIterator = historyManager.iterateChangesets(targetRef, headRef(tmpNodeId));
    while (tmpChangesetIterator.hasNext()) {
      Changeset changeset = tmpChangesetIterator.next();
      //Use the next version of the target branch as the new version for the commit
      long newVersion = mergeCommitRef.getVersion();
      //Use the resolved target ref as base ref here, to ensure consistency, because there could be even incoming changes into the target branch during the merge process
      //TODO: If a conflict happens at this stage the complete merge has to be retried again from scratch and ensure to not write a partial version here (implement the copy process inside a DB function instead?)
      writeCommit(targetNodeId, changeset.toModifications(OnVersionConflict.MERGE, OnMergeConflict.ERROR), changeset.getAuthor(), targetRef, newVersion);
    }

    return finalizeMerge(branchHeadRef, baseRef, tmpNodeId, mergeCommitRef);
  }

  private MergeOperationResult finalizeMerge(Ref branchHeadRef, Ref baseRef, int tmpNodeId, Ref mergeCommitRef) throws SQLException {
    deleteBranch(tmpNodeId);
    return new MergeOperationResult(getNodeId(branchHeadRef), baseRef, false, branchHeadRef.getVersion(), mergeCommitRef);
  }

  //TODO: support operations: revert, merge, squash (one / multiple / all commits), cherry-pick (one / multiple / all commits), drop (a commit)

  private Ref findBaseRefInCommonAncestor(Ref sourceRef, Ref targetRef) throws SQLException {
    int sourceNodeId = getNodeId(sourceRef);
    int targetNodeId = getNodeId(targetRef);
    if (sourceNodeId == targetNodeId)
      return sourceRef;
    List<Ref> sourceBranchPath = branchPath(sourceNodeId);
    List<Ref> targetBranchPath = branchPath(targetNodeId);
    return findBaseRefInCommonAncestor(sourceBranchPath, targetBranchPath);
  }

  private Ref findBaseRefInCommonAncestor(List<Ref> sourceBranchPath, List<Ref> targetBranchPath) {
    if (targetBranchPath.isEmpty())
      //The common ancestor is the main branch
      return sourceBranchPath.get(0);

    //Check if the nearest base ref of the source is an ancestor of the target ref
    Ref baseRef = sourceBranchPath.get(sourceBranchPath.size() - 1);
    if (Lists.reverse(targetBranchPath).stream().anyMatch(targetRef -> getNodeId(baseRef) == getNodeId(targetRef)))
      return baseRef;

    //Check with the next base ref of the source
    return findBaseRefInCommonAncestor(sourceBranchPath.subList(0, sourceBranchPath.size() - 1), targetBranchPath);
  }

  public void prune(Map<Integer, Ref> existingBranches) { //TODO: 1.) Needed at all? Even if base branches of branches to be deleted would be automatically checked then and deleted together?
    /*
    TODO:
     0.) Gather a root table lock from service side directly, before commanding the prune (which also needs to be gathered *during* (but not before necessarily) all writing branch operations; e.g. createBranch, rebase, ...) to make sure no branch table gets deleted which was created since the prune was commanded (concurrently)
     1.) Delete all tables which are not referenced at all anymore. Find these tables by doing the following
      1.1) Add all keys of the map to a "safe-list"
      1.2) Iterate through the existingBranches map and check for each entry, whether its base is also in that map, if not (and if the base is not 0 [main]) add the base to an "orphaned-safe-list"
      1.3) Load all existing branch table names into an "all-branches-list"
      1.4) For each element in the "orphaned-safe-list", find the according entry in the "all-branches-list" and gather its base node ID
      1.5) If that base node ID is not 0 and is not in none of the safe-lists, add it to a "transitive-orphaned-safe-list"
      1.6) Add all items of the "orphaned-safe-list" to the safe-list and make the "transitive-orphaned-safe-list" being the new "orphaned-safe-list"
      1.7) If the "orphaned-safe-list" is not empty now, goto point 1.4) again
      1.8) If the "orphaned-safe-list" is empty now, delete all tables of which the node ID (not base node ID!) is not in the "safe-list"
     2.) Prune all versions in the main branch, on which no branch is based and which may be deleted with respect to v2k by doing the following
      2.1) The list of remaining existing branches which have not been deleted in step 1.) are still in memory, find all branches which are based on the main branch (node ID 0)
      2.2) Of these branches, find the one with the lowest base version
      2.3) Prune all versions which are lower than that lowest base version AND which are older than the oldest allowed version with respect to v2k
     3.) Delete all the versions in orphaned-branches, which are not needed anymore, because branches are only based on older versions in that orphaned branch by doing the following
      3.1) For all orphaned-branches (kept in memory from step 1), find the branching branches, with the highest base version
      3.2) Delete all versions from all each of the orphaned-branches which are higher than the according highest base version of the respective branching branch
     */
  }

  public void deleteBranch(int nodeId) throws SQLException {
    /*
    NOTE: Only delete the branch if no other branches are based on it.
    Once no branch is based on the branch anymore and no ref is pointing to the node anymore, the next prune will delete the table.
     */
    if (!hasBranches(nodeId))
      new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table}")
          .withVariable("schema", schema)
          .withVariable("table", branchTableName(nodeId))
          .write(dataSourceProvider);
  }

  public void deleteAllBranchTables() throws SQLException {
    List<String> branchTableNames = new SQLQuery("SELECT tablename FROM pg_tables WHERE schemaname = #{schema} AND tablename ~ #{tablePattern}")
        .withNamedParameter(SCHEMA, schema)
        .withNamedParameter("tablePattern", rootTable + "_[0-9_]+_[0-9_]+_[0-9_]+$")
        .run(dataSourceProvider, rs -> {
          List<String> tableNames = new ArrayList<>();
          while (rs.next())
            tableNames.add(rs.getString("tablename"));
          return tableNames;
        });

    if (!branchTableNames.isEmpty())
      SQLQuery.batchOf(branchTableNames.stream()
          .map(branchTable -> new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table}")
              .withVariable(SCHEMA, schema)
              .withVariable("table", branchTable))
          .toList())
          .writeBatch(dataSourceProvider);
  }

  /**
   * Returns true, if the branch specified by the node ID has one or multiple branches that are based on it.
   *
   * @param nodeId The node ID of the potential base branch
   * @return True, if other branches are based on the branch
   */
  private boolean hasBranches(int nodeId) throws SQLException {
    String tablePattern = basePrefix(nodeId) + "%_%";
    //TODO: Rather use a regex instead of 2 checks
    return new SQLQuery("""
        SELECT FROM pg_tables WHERE schemaname = #{schema}
        AND tablename LIKE #{tablePattern}
        AND NOT tablename LIKE #{partitionsPattern}
        """)
        .withNamedParameter(SCHEMA, schema)
        .withNamedParameter("tablePattern", toLikePattern(tablePattern))
        .withNamedParameter("partitionsPattern", toLikePattern(tablePattern + "_%"))
        .run(dataSourceProvider, rs -> rs.next());
  }

  private String toLikePattern(String pattern) {
    return pattern.replaceAll("_", "\\_");
  }

  private String branchTableName(int nodeId) throws SQLException {
    //TODO: Cache the results
    if (nodeId == 0)
      return rootTable;
    return new SQLQuery("SELECT tablename FROM pg_tables WHERE schemaname = #{schema} AND tablename LIKE #{tablePattern}")
        .withNamedParameter(SCHEMA, schema)
        .withNamedParameter("tablePattern", toLikePattern(rootTable + "_%_" + nodeId))
        .run(dataSourceProvider, rs -> {
          if (rs.next())
            return rs.getString("tablename");
          throw new SQLException("Unable to find table for node ID " + nodeId);
        });
  }

  private String branchTableName(int baseNodeId, long baseVersion, int nodeId) {
    return branchTableName(rootTable, baseNodeId, baseVersion, nodeId);
  }

  public String branchTableName(Ref baseRef, int nodeId) {
    return branchTableName(rootTable, baseRef, nodeId);
  }

  public static String branchTableName(String rootTable, Ref baseRef, int nodeId) {
    return branchTableName(rootTable, getNodeId(baseRef), baseRef.getVersion(), nodeId);
  }

  private static String branchTableName(String rootTable, int baseNodeId, long baseVersion, int nodeId) {
    //Branch table names are looking like: <root table name>_<base node ID>_<base version>_<node ID>
    return rootTable + "_" + baseNodeId + "_" + baseVersion + "_" + nodeId;
  }

  public static int getNodeId(Ref ref) {
    if (!ref.getBranch().startsWith("~"))
      throw new IllegalArgumentException("Cannot parse ref that is not resolved to a node ID. Provided ref: " + ref);

    return Integer.parseInt(ref.getBranch().substring(1));
  }

  /**
   * Returns the prefix for the table name that other branches used to branch the branch depicted by the specified node ID.
   *
   * @param nodeId The node ID of the branch
   * @return The prefix for the table names of other branches based on the branch
   */
  private String basePrefix(int nodeId) {
    //NOTE: The underscore at the end is very important to not match other node IDs which have the same starting digits
    return rootTable + "_" + nodeId + "_";
  }

  private int baseNodeId(String branchTableName) {
    return Integer.parseInt(branchTableName.split("_")[1]);
  }

  private long baseVersion(String branchTableName) {
    return Long.parseLong(branchTableName.split("_")[2]);
  }

  private int nodeId(String branchTableName) {
    return Integer.parseInt(branchTableName.split("_")[3]);
  }

  private Ref baseRef(int nodeId) throws SQLException {
    String branchTableName = branchTableName(nodeId);
    return new Ref("~" + baseNodeId(branchTableName) + ":" + baseVersion(branchTableName));
  }

  List<Ref> branchPath(int nodeId) throws SQLException {
    return branchPath(MAIN_BRANCH.getNodeId(), nodeId);
  }

  /**
   * Creates the path of all baseRefs for a branch depicted by the nodeId,
   * starting from the branch depicted by the startNodeId and ending at the base ref of the specified nodeId.
   *
   * @param startNodeId The node ID of the branch from which to start to gather the involved branch tables
   * @param nodeId The nodeId that points to the last branch
   * @return A list of base refs of all involved branches between the specified startNodeId and endNodeId
   */
  List<Ref> branchPath(int startNodeId, int nodeId) throws SQLException {
    if (nodeId == startNodeId)
      return List.of();
    List<Ref> branchPath = new ArrayList<>();
    Ref baseRef = baseRef(nodeId);
    branchPath.addAll(branchPath(getNodeId(baseRef)));
    branchPath.add(baseRef);
    return branchPath;
  }

  public static Ref headRef(int nodeId) {
    return Ref.fromBranchId("~" + nodeId);
  }

  public Ref resolveHead(Ref ref) throws SQLException {
    return !ref.isHead() ? ref : Ref.fromBranchId(ref.getBranch(), loadHeadVersion(getNodeId(ref)));
  }

  private long loadHeadVersion(int nodeId) throws SQLException {
    Ref baseRef = nodeId == 0 ? new Ref(0) : baseRef(nodeId);
    long tableMaxVersion = loadMaxVersion(nodeId);
    return tableMaxVersion + baseRef.getVersion();
  }

  private long loadMaxVersion(int nodeId) throws SQLException {
    return new SQLQuery("SELECT max(version) FROM ${schema}.${table}")
        .withVariable(SCHEMA, schema)
        .withVariable(TABLE, branchTableName(nodeId))
        .run(dataSourceProvider, rs -> rs.next() ? rs.getLong("max") : 0);
  }

  private int getNewNodeId() throws SQLException {
    //TODO: For existing spaces which do not have a sequence yet, create it on-demand
    return getNextSequenceValue(rootTable, "branches");
  }

  private long getNextVersion(int nodeId) throws SQLException {
    //TODO: Overload this method with a signature that contains a baseRef parameter (for better performance)
    return getNextSequenceValue(branchTableName(nodeId), "version");
  }

  private void setNextVersion(int nodeId, Ref baseRef, long nextVersion) throws SQLException {
    setNextSequenceValue(branchTableName(baseRef, nodeId), "version", nextVersion);
  }

  private Integer getNextSequenceValue(String tableName, String sequenceName) throws SQLException {
    return new SQLQuery("SELECT nextval('${schema}.${sequence}')")
        .withVariable(SCHEMA, schema)
        .withVariable("sequence", sequenceName(tableName, sequenceName))
        .run(dataSourceProvider, rs -> {
          if (rs.next())
            return rs.getInt(1);
          throw new SQLException("Unable to increase " + sequenceName + " sequence.");
        });
  }

  private void setNextSequenceValue(String tableName, String sequenceName, long newValue) throws SQLException {
    new SQLQuery("SELECT setval('${schema}.${sequence}', #{newValue}, true)")
        .withVariable(SCHEMA, schema)
        .withVariable("sequence", sequenceName(tableName, sequenceName))
        .withNamedParameter("newValue", newValue)
        .run(dataSourceProvider);
  }

  public interface BranchOperationResult {
    int nodeId();
    Ref baseRef();
    boolean conflicting();
  }

  public record BranchOpResult(int nodeId, Ref baseRef, boolean conflicting) implements BranchOperationResult {}

  public record MergeOperationResult(int nodeId, Ref baseRef, boolean conflicting, long mergedSourceVersion, Ref resolvedMergeTargetRef)
      implements BranchOperationResult {}
}
