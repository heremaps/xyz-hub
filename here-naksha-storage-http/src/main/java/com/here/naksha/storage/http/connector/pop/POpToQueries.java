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
package com.here.naksha.storage.http.connector.pop;

import static com.here.naksha.lib.core.models.storage.OpType.AND;

import com.here.naksha.lib.core.models.payload.events.PropertyQueryOr;
import com.here.naksha.lib.core.models.payload.events.TagsQuery;
import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.PRef;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class POpToQueries {

  private static final int MAX_EXPECTED_POP_DEPTH = 5;

  public static POpQueries pOpToQuery(POp pOp) {
    if (pOp.op() == AND) {
      List<POp> pOpChildren = Objects.requireNonNull(pOp.children(), "AND operation must have children");

      Optional<TagsQuery> tagQueries = pOpChildren.stream()
          .filter(POpToQueries::isTagPOp)
          .reduce(POp::and)
          .map(POpToTagsQuery::toTagsQuery);

      Optional<PropertyQueryOr> propertyQuery = pOpChildren.stream()
          .filter(POpToQueries::isPropertyPOp)
          .reduce(POp::and)
          .map(POpToPropertiesQuery::toPopQueryOr);

      return new POpQueries(propertyQuery, tagQueries);
    } else if (isPropertyPOp(pOp)) {
      return new POpQueries(Optional.of(POpToPropertiesQuery.toPopQueryOr(pOp)), Optional.empty());
    } else if (isTagPOp(pOp)) {
      return new POpQueries(Optional.empty(), Optional.of(POpToTagsQuery.toTagsQuery(pOp)));
    }
    throw new IllegalStateException("Should not reach here");
  }

  /**
   * Assumes that if one leaf of tree is Tag POp, the whole tree is a Tag POp
   * Assumes that trees are not deeper than {@link POpToQueries#MAX_EXPECTED_POP_DEPTH}
   * to avoid stack overflow.
   */
  private static boolean isTagPOp(POp pOp) {
    return isTagPOp(pOp, MAX_EXPECTED_POP_DEPTH);
  }

  private static boolean isPropertyPOp(POp pOp) {
    return !isTagPOp(pOp, MAX_EXPECTED_POP_DEPTH);
  }

  private static boolean isTagPOp(POp pOp, int maxSearchDepth) {
    if (maxSearchDepth == 0) throw new IllegalStateException("Cannot handle tree this deep");
    PRef propertyRef = pOp.getPropertyRef();
    if (propertyRef == null) {
      List<POp> pOpChildren =
          Objects.requireNonNull(pOp.children(), "POp without property reference must have children");
      return isTagPOp(pOpChildren.get(0), maxSearchDepth - 1);
    } else {
      return propertyRef.getTagName() != null;
    }
  }

  public record POpQueries(Optional<PropertyQueryOr> propertyQuery, Optional<TagsQuery> tagsQuery) {}
}
