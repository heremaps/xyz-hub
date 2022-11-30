/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz.models.txn;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.XyzSerializable;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "TransactionLog")
@SuppressWarnings({"unused", "WeakerAccess"})
public class TransactionLog implements XyzSerializable {

  private String space_id;
  private List<String> uuids;
  private List<TransactionData> txnDataList;

  /**
   * Create a new empty object.
   */
  public TransactionLog() {
    super();
  }

  public String getSpace_id() {
    return space_id;
  }

  public void setSpace_id(String space_id) {
    this.space_id = space_id;
  }

  public List<String> getUuids() {
    return uuids;
  }

  public void setUuids(List<String> uuids) {
    this.uuids = uuids;
  }

  public List<TransactionData> getTxnDataList() {
    return txnDataList;
  }

  public void setTxnDataList(List<TransactionData> txnDataList) {
    this.txnDataList = txnDataList;
  }

  @Override
  public String toString() {
    return "TransactionLog{" +
            "space_id='" + space_id + '\'' +
            ", uuids=" + uuids +
            ", txnDataList=" + txnDataList +
            '}';
  }
}
