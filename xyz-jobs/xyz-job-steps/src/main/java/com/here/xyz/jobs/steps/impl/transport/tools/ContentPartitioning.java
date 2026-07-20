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

package com.here.xyz.jobs.steps.impl.transport.tools;

import com.here.xyz.util.db.SQLQuery;

/**
 * Shared helpers to split space-based work (e.g. copy or count) into disjoint partitions
 * that can be processed in parallel.
 */
public final class ContentPartitioning {

  private ContentPartitioning() {}

  /**
   * Builds a filter fragment that assigns each feature to exactly one of {@code threadCount} disjoint
   * partitions based on a hash of the internal {@code i} column, so the work can be split across
   * multiple parallel tasks whose partial results together cover every feature exactly once.
   *
   * @param threadCount The total number of partitions.
   * @param threadId    The zero-based index of the partition to build the filter for.
   * @return A filter {@link SQLQuery} fragment, or {@code null} if no restriction is needed
   *         (i.e. a single partition covering everything).
   */
  public static SQLQuery buildThreadIdFilter(int threadCount, int threadId) {
    if (threadCount <= 1)
      return null;

    final long VIZ_ID_COUNT = 0xfffffL + 1,
               blockRange = (long) Math.ceil((double) VIZ_ID_COUNT / (double) threadCount);

    final String VIZ_IDX_FKT = "left(md5((''::text || i)), 5)";

    SQLQuery lowerBoundCondition = null,
             upperBoundCondition = null;

    if (threadId > 0)
      lowerBoundCondition = new SQLQuery("${{vizIdxFkt1}} >= #{vizLowerBound}")
          .withQueryFragment("vizIdxFkt1", VIZ_IDX_FKT)
          .withNamedParameter("vizLowerBound", String.format("%05x", threadId * blockRange));

    if (threadId < threadCount - 1)
      upperBoundCondition = new SQLQuery("${{vizIdxFkt2}} < #{vizUpperBound}")
          .withQueryFragment("vizIdxFkt2", VIZ_IDX_FKT)
          .withNamedParameter("vizUpperBound", String.format("%05x", (threadId + 1) * blockRange));

    if (lowerBoundCondition == null && upperBoundCondition == null)
      return null;

    SQLQuery additionalFilterFragment = new SQLQuery(
        (lowerBoundCondition != null && upperBoundCondition != null) ? "${{Bound1}} AND ${{Bound2}}" : "${{Bound1}}");

    if (lowerBoundCondition != null)
      additionalFilterFragment.withQueryFragment("Bound1", lowerBoundCondition);

    if (upperBoundCondition != null)
      additionalFilterFragment.withQueryFragment(lowerBoundCondition != null ? "Bound2" : "Bound1", upperBoundCondition);

    return additionalFilterFragment;
  }
}
