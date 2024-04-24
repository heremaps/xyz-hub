/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.naksha.lib.core.models.storage;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.naksha.lib.core.LibraryConstants;
import com.here.naksha.lib.core.NakshaVersion;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/**
 * All read requests should extend this base class.
 *
 * @param <SELF> the self-type.
 */
@AvailableSince(NakshaVersion.v2_0_7)
@JsonSubTypes({
  @JsonSubTypes.Type(value = ReadFeatures.class),
  @JsonSubTypes.Type(value = ReadCollections.class),
  @JsonSubTypes.Type(value = ReadTransactionsByComment.class),
  @JsonSubTypes.Type(value = ReadTransactionsForSequence.class),
  @JsonSubTypes.Type(value = ReadTransactionsByTxn.class)
})
public class ReadRequest<SELF extends ReadRequest<SELF>> extends Request<SELF> {

  // We do not make the class abstract, so that Jackson can create an instance to fiddle out the default value of
  // properties.
  protected ReadRequest() {}

  /**
   * The amount of features to fetch at ones from the storage. The storage should use this as a strong hint, still, the storage may use
   * different values on demand.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonInclude(Include.NON_DEFAULT)
  @JsonProperty
  protected int fetchSize = 1000;

  /**
   * The total amount of features to fetch from the storage. This value will be included in SELECT statement.
   * Default value is {@link LibraryConstants#DEFAULT_READ_LIMIT}.
   * Unlimited read - set limit to null to drop limitation, but be aware that only {@link ForwardCursor} will read rows one by another
   * (actually it will fetch rows in packets of size defined by {@link #fetchSize}). Whenever you use {@link MutableCursor}
   * you will instantly fetch all rows to memory.
   */
  @AvailableSince(NakshaVersion.v2_0_9)
  @JsonInclude(Include.NON_DEFAULT)
  @JsonProperty
  protected Long limit = LibraryConstants.DEFAULT_READ_LIMIT;

  public int getFetchSize() {
    return fetchSize;
  }

  public Long getLimit() {
    return limit;
  }
}
