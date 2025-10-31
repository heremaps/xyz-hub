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

import static com.here.xyz.psql.query.branching.BranchManager.branchTableName;
import static com.here.xyz.psql.query.branching.BranchManager.getNodeId;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.WriteFeaturesEvent.Modification;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.util.db.pg.FeatureWriterQueryBuilder;
import com.here.xyz.util.db.pg.FeatureWriterQueryBuilder.FeatureWriterQueryContextBuilder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class CommitManager {

  protected final BranchManager branchManager;

  public CommitManager(BranchManager branchManager) {
    this.branchManager = branchManager;
  }

  public SimpleCommitResult writeCommit(int nodeId, Set<Modification> modifications, String author, Ref baseRef) throws SQLException {
    return writeCommit(nodeId, modifications, author, baseRef, -1);
  }

  public SimpleCommitResult writeCommit(int nodeId, Set<Modification> modifications, String author, Ref baseRef, long version) throws SQLException {
    List<Ref> branchPath = branchManager.branchPath(0, nodeId);
    List<String> tables = branchPathToTableChain(branchManager.rootTable, branchPath, nodeId);
    List<Long> tableBaseVersions = new ArrayList<>();
    tableBaseVersions.add(0l);
    tableBaseVersions.addAll(branchPath.stream().map(ref -> ref.getVersion()).collect(Collectors.toList()));
    return writeCommit(tables, tableBaseVersions, modifications, author, baseRef.isHead() ? -1 : baseRef.getVersion(), version);
  }

  private SimpleCommitResult writeCommit(List<String> tables, List<Long> tableBaseVersions, Set<Modification> modifications, String author,
      long baseVersion, long version) throws SQLException {
    FeatureWriterQueryContextBuilder queryContextBuilder = new FeatureWriterQueryContextBuilder()
        .withSchema(branchManager.schema)
        .withTables(tables)
        .withTableBaseVersions(tableBaseVersions)
        .withHistoryEnabled(true)
        .withBatchMode(true);
    if (baseVersion != -1)
      queryContextBuilder.withBaseVersion(baseVersion);

    return new FeatureWriterQueryBuilder()
        .withModifications(new ArrayList<>(modifications))
        .withAuthor(author)
        .withVersion(version)
        .withQueryContext(queryContextBuilder.build())
        .withSelect(true)
        .withReturnResult(false)
        .build()
        .withLoggingEnabled(false)
        .run(branchManager.dataSourceProvider, rs -> {
          try {
            if (!rs.next())
              throw new SQLException("Illegal result was returned from FeatureWriter.");
            return XyzSerializable.deserialize(rs.getString(1), SimpleCommitResult.class);
          }
          catch (JsonProcessingException e) {
            throw new SQLException("JSON Result could not be parsed.", e);
          }
        });
  }

  /**
   * Constructs the table chain out of the provided branchPath.
   * The table chain is the list of tables to be used for writing when using the FeatureWriter to "commit" a new version.
   * It's necessary to allow the FeatureWriter to check the existence & state of features in the base branches.
   * @param rootTable The top level root table of the space
   * @param branchPath The branch path containing a chain of base branches of the branch into which to write
   *  (starting with the first branch after the main branch)
   * @param endNodeId The node ID of the last / "HEAD" branch - the branch into which to write
   * @return The "table chain" - A list of tables to be used for the FeatureWriter
   */
  public static LinkedList<String> branchPathToTableChain(String rootTable, List<Ref> branchPath, int endNodeId) {
    LinkedList<String> tableChain = new LinkedList<>();
    int nodeId = endNodeId;

    for (Ref baseRef : Lists.reverse(branchPath)) {
      tableChain.addFirst(branchTableName(rootTable, baseRef, nodeId));
      nodeId = getNodeId(baseRef);
    }

    if (nodeId == 0)
      tableChain.addFirst(rootTable);

    return tableChain;
  }

  public static class SimpleCommitResult {
    private final int count;
    private final int conflicting;

    public SimpleCommitResult(int count, int conflicting) {
      this.count = count;
      this.conflicting = conflicting;
    }

    public int count() { return count; }
    public int conflicting() { return conflicting; }
  }
}
