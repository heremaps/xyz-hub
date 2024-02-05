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

import static com.here.xyz.models.hub.Ref.HEAD;
import static com.here.xyz.psql.query.branching.BranchManager.getNodeId;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.psql.query.IterateChangesets;
import com.here.xyz.responses.changesets.Changeset;
import com.here.xyz.responses.changesets.ChangesetCollection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HistoryManager {
  protected final BranchManager branchManager;

  public HistoryManager(BranchManager branchManager) {
    this.branchManager = branchManager;
  }

  public ChangesetIterator iterateChangesets(Ref startRef, Ref endRef) throws SQLException {
    List<Ref> branchPath = branchManager.branchPath(getNodeId(startRef), getNodeId(endRef)); //TODO: Check if we can re-use maybe a subset of the branchPath of the incoming event
    return new ChangesetIterator(branchPath,
        new Ref(endRef.getBranch() + ":" + startRef.getVersion() + ".." + (endRef.isHead() ? HEAD : endRef.getVersion())));
  }

  /**
   * NOTE: This iterator provides consecutive changeset-objects that are having the same version number.
   * These changeset-objects are actually just "pages" of the same changeset.
   */
  public class ChangesetIterator implements Iterator<Changeset> {
    private final List<Ref> branchPath;
    private final Ref rangeRef;
    private ChangesetCollection changesets;
    private Iterator<Changeset> collectionIterator;

    private ChangesetIterator(List<Ref> branchPath, Ref rangeRef) {
      this.branchPath = branchPath;
      this.rangeRef = rangeRef;
    }

    private void initCollectionIterator() {
      collectionIterator = changesets.getVersions().entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .map(Map.Entry::getValue)
          .iterator();
    }

    private void loadNextPage() {
      IterateChangesetsEvent event = new IterateChangesetsEvent()
          .withSpace(branchManager.spaceId)
          .withRef(rangeRef)
          .withBranchPath(branchPath)
          .withLimit(100)
          .withNextPageToken(changesets == null ? null : changesets.getNextPageToken())
          .withNodeId(getNodeId(rangeRef))
          .withStreamId(branchManager.streamId)
          .withVersionsToKeep(1000); //TODO: Read from original event?
          //.withMinVersion() //TODO:
          //.withConnectorParams() //TODO:
          //.withParams(); //TODO:

      try {
        changesets = new IterateChangesets(event)
            .withDataSourceProvider(branchManager.dataSourceProvider)
            .run();
      }
      catch (ErrorResponseException | SQLException e) {
        throw new RuntimeException("Unexpected error while iterating changesets: ", e);
      }
      initCollectionIterator();
    }

    @Override
    public boolean hasNext() {
      if (collectionIterator == null || !collectionIterator.hasNext() && changesets.getNextPageToken() != null)
        loadNextPage();
      return collectionIterator.hasNext() || changesets.getNextPageToken() != null;
    }

    @Override
    public Changeset next() {
      if (collectionIterator == null || !collectionIterator.hasNext() && changesets.getNextPageToken() != null)
        loadNextPage();
      return collectionIterator.next();
    }
  }
}
