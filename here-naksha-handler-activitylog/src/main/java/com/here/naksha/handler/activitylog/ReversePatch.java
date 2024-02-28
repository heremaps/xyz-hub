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
package com.here.naksha.handler.activitylog;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.util.diff.RemoveOp;
import com.here.naksha.lib.core.util.diff.UpdateOp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public record ReversePatch(
    @JsonProperty(PatchOp.ADD) int insert,
    @JsonProperty(PatchOp.REMOVE) int remove,
    @JsonProperty(PatchOp.REPLACE) int update,
    List<PatchOp> ops) {

  static ReversePatch EMPTY = new ReversePatch(0, 0, 0, Collections.emptyList());

  public record PatchOp(String op, String path, @JsonInclude(Include.NON_NULL) Object value) {

    static final String REMOVE = "remove";
    static final String ADD = "add";

    static final String REPLACE = "replace";

    static PatchOp remove(String path) {
      return new PatchOp(REMOVE, path, null);
    }

    static PatchOp insert(String path, Object value) {
      return new PatchOp(ADD, path, value);
    }

    static PatchOp update(String path, Object newValue) {
      return new PatchOp(REPLACE, path, newValue);
    }

    @Override
    public String toString() {
      return "ReverseOp{" + "op='" + op + '\'' + ", path='" + path + '\'' + ", value=" + value + '}';
    }
  }

  static Builder builder() {
    return new Builder();
  }

  static class Builder {

    int insert;
    int remove;
    int update;
    List<PatchOp> ops;

    private Builder() {
      insert = 0;
      remove = 0;
      update = 0;
      ops = new ArrayList<>();
    }

    ReversePatch build() {
      return new ReversePatch(insert, remove, update, ops);
    }

    Builder reverseInsert(String path) {
      remove++;
      ops.add(PatchOp.remove(path));
      return this;
    }

    Builder reverseRemove(RemoveOp removeOp, String path) {
      insert++;
      ops.add(PatchOp.insert(path, removeOp.oldValue()));
      return this;
    }

    Builder reverseUpdate(UpdateOp updateOp, String path) {
      update++;
      ops.add(PatchOp.update(path, updateOp.oldValue()));
      return this;
    }
  }
}
