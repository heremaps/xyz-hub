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

package com.here.xyz.hub.task;

import static com.here.xyz.hub.task.ModifyOp.IfExists.DELETE;
import static com.here.xyz.hub.task.ModifyOp.IfExists.MERGE;
import static com.here.xyz.hub.task.ModifyOp.IfExists.PATCH;
import static com.here.xyz.hub.task.ModifyOp.IfExists.REPLACE;
import static com.here.xyz.hub.task.ModifyOp.IfExists.RETAIN;

import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.task.ModifyOp.Entry;
import com.here.xyz.hub.util.diff.Difference;
import com.here.xyz.hub.util.diff.Patcher;
import com.here.xyz.hub.util.diff.Patcher.ConflictResolution;
import io.vertx.core.json.JsonObject;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A modify operation
 */
public abstract class ModifyOp<T, K extends Entry<T>> {

  public final List<K> entries;
  public final boolean isTransactional;

  private Set<Operation> usedOperations;

  public enum Operation {
    READ,
    CREATE,
    UPDATE,
    DELETE,
    WRITE //Reserved for future use
  }

  private static final List<IfExists> UPDATE_OPS = Arrays.asList(PATCH, MERGE, REPLACE);

  public ModifyOp(List<K> entries, boolean isTransactional) {
    this.isTransactional = isTransactional;
    this.entries = entries;
  }

  public Set<Operation> getUsedOperations() {
    if (usedOperations == null) {
      usedOperations = new HashSet<>();
      entries.forEach(e -> {
        if (e.head != null && e.ifExists.equals(RETAIN))
          usedOperations.add(Operation.READ);
        else if (e.head == null && e.ifNotExists.equals(IfNotExists.CREATE))
          usedOperations.add(Operation.CREATE);
        else if (e.head != null && e.ifExists.equals(DELETE))
          usedOperations.add(Operation.DELETE);
        else if (e.head != null && UPDATE_OPS.contains(e.ifExists))
          usedOperations.add(Operation.UPDATE);
      });
    }
    if (!Collections.disjoint(usedOperations, Arrays.asList(Operation.CREATE, Operation.UPDATE, Operation.DELETE)))
      usedOperations.add(Operation.WRITE);
    return usedOperations;
  }

  public boolean isRead() {
    return getUsedOperations().contains(Operation.READ);
  }

  public boolean isCreate() {
    return getUsedOperations().contains(Operation.CREATE);
  }

  public boolean isDelete() {
    return getUsedOperations().contains(Operation.DELETE);
  }

  public boolean isUpdate() {
    return getUsedOperations().contains(Operation.UPDATE);
  }

  public boolean isWrite() { //Reserved for future use
    return getUsedOperations().contains(Operation.WRITE);
  }

  /**
   * Applies the modifications provided by the input to the source state, both provided with the input, and produces the target state as
   * output.
   *
   * @throws ModifyOpError when a processing error occurs.
   */
  public void process() throws ModifyOpError, HttpException {
    for (K entry : entries) {
      try {
        //IF NOT EXISTS
        if (entry.head == null) {
          switch (entry.ifNotExists) {
            case RETAIN:
              entry.result = null;
              break;
            case CREATE: {
              entry.result = entry.create();
              break;
            }
            case ERROR:
              throw new ModifyOpError("The record does not exist.");
          }
        }
        //IF EXISTS
        else {
          switch (entry.ifExists) {
            case RETAIN:
              entry.result = entry.transform();
              break;
            case MERGE:
              entry.result = entry.merge();
              break;
            case PATCH:
              entry.result = entry.patch();
              break;
            case REPLACE:
              entry.result = entry.replace();
              break;
            case DELETE:
              entry.result = entry.delete();
              break;
            case ERROR:
              throw new ModifyOpError("The record {" + entry.getId(entry.head) + "} exists.");
          }
        }

        //Check if the isModified flag is not already set. Compare the objects in case it is not set yet.
        entry.isModified = entry.isModified || entry.isModified();
      }
      catch (ModifyOpError e) {
        if (isTransactional) {
          throw e;
        }
        //TODO: Check if this is included in the failed array
        entry.exception = e;
      }
    }
  }

  public enum IfExists {
    RETAIN,
    ERROR,
    DELETE,
    REPLACE,
    PATCH,
    MERGE;

    public static IfExists of(String value) {
      if (value == null) {
        return null;
      }
      try {
        return valueOf(value.toUpperCase());
      } catch (IllegalArgumentException e) {
        return null;
      }
    }
  }

  public enum IfNotExists {
    RETAIN,
    ERROR,
    CREATE;

    public static IfNotExists of(String value) {
      if (value == null) {
        return null;
      }
      try {
        return valueOf(value.toUpperCase());
      } catch (IllegalArgumentException e) {
        return null;
      }
    }
  }

  public static abstract class Entry<T> {

    /**
     * The input as it comes from the caller
     */
    public Map<String, Object> input;

    /**
     * The latest state the object currently has in the data storage
     */
    public T head;

    /**
     * The state onto which the caller made the modifications
     */
    public T base;

    /**
     * The resulting target state which should go to the data storage after merging
     */
    public T result;

    public final IfExists ifExists;
    public final IfNotExists ifNotExists;

    private final ConflictResolution cr;
    private Map<String, Object> headMap;
    public boolean isModified;
    public Exception exception;
    public long inputVersion;
    public boolean skipConflictDetection;
    private Map<String, Object> resultMap;

    public Entry(Map<String, Object> input, IfNotExists ifNotExists, IfExists ifExists, ConflictResolution cr) {
      this.inputVersion = getVersion(input);
      this.input = filterMetadata(input);
      this.cr = cr;
      this.ifExists = ifExists;
      this.ifNotExists = ifNotExists;
    }

    private Map<String, Object> getHeadMap() throws HttpException, ModifyOpError {
      if (headMap == null && head != null) {
          headMap = toMap(head);
      }
      return headMap;
    }

    private Map<String, Object> getResultMap() throws HttpException, ModifyOpError {
      if (resultMap == null && result!=null) {
        resultMap = toMap(result);
      }
      return resultMap;
    };

    protected abstract String getId(T record);

    protected abstract long getVersion(Map<String, Object> record);

    protected abstract long getVersion(T record);

    public T patch() throws ModifyOpError, HttpException {
      final Map<String, Object> computedInput = toMap(base);
      final Difference diff = Patcher.calculateDifferenceOfPartialUpdate(computedInput, input, null, true);
      if (diff == null) {
        return head;
      }

      Patcher.patch(computedInput, diff);
      input = computedInput;
      return merge();
    }

    public T merge() throws ModifyOpError, HttpException {
      if (base.equals(head)) {
        return replace();
      }

      final Map<String, Object> resultMap = toMap(base);

      final Difference diffInput = Patcher.getDifference(resultMap, input);
      if (diffInput == null) {
        return head;
      }
      final Difference diffHead = Patcher.getDifference(resultMap, getHeadMap());
      try {
        final Difference mergedDiff = Patcher.mergeDifferences(diffHead, diffInput, cr);
        Patcher.patch(resultMap, mergedDiff);
        this.resultMap = resultMap;
        return fromMap(resultMap);
      }
      catch (Exception e) {
        throw new ModifyOpError(e.getMessage());
      }
    }

    public T replace() throws ModifyOpError, HttpException {
      if (!skipConflictDetection && inputVersion != -1 && getVersion(head) != -1 && inputVersion != getVersion(head))
        throw new ModifyOpError("The feature with id " + getId(head) + " cannot be replaced. The provided version doesn't match the "
            + "version of the head state: " + getVersion(head));

      return fromMap(input);
    }

    public T delete() throws ModifyOpError, HttpException {
      if (!skipConflictDetection && inputVersion != -1 && getVersion(head) != -1 && inputVersion != getVersion(head))
        throw new ModifyOpError("The feature with id " + getId(head) + " cannot be deleted. The provided version doesn't match the "
            + "version of the head state: " + getVersion(head));

      if (head != null)
        isModified = true;

      return null;
    }

    public T create() throws ModifyOpError, HttpException {
      isModified = true;
      return fromMap(input);
    }

    public T transform() {
      return head;
    }

    public abstract Map<String, Object> filterMetadata(Map<String, Object> map);

    public abstract T fromMap(Map<String, Object> map) throws ModifyOpError, HttpException;

    public abstract Map<String, Object> toMap(T record) throws ModifyOpError, HttpException;

    /**
     * Checks, if the result of the operation is different from the head state.
     *
     * @return true, if the result of the operation is different from the head state.
     */
    boolean isModified() throws HttpException, ModifyOpError {
      if (Objects.equals(head, result)) {
        return false;
      }
      return !(new JsonObject(getHeadMap()).equals(new JsonObject(getResultMap())));
    }
  }

  public static class ModifyOpError extends Exception {

    public ModifyOpError(String msg) {
      super(msg);
    }
  }

  /**
   * Filter out recursively all properties from the provided filter, where the value is set to 'true'.
   *
   * @param map the object to filter
   * @param filter the filter to apply
   */
  public static Map<String, Object> filter(Map<String, Object> map, Map<String, Object> filter) {
    if (map == null || filter == null) {
      return map;
    }

    for (String key : filter.keySet()) {
      if (filter.get(key) instanceof Map && map.get(key) instanceof Map) {
        //noinspection unchecked
        filter((Map<String, Object>) map.get(key), (Map<String, Object>) filter.get(key));
      }
      if (filter.get(key) instanceof Boolean && (Boolean) filter.get(key)) {
        map.remove(key);
      }
    }
    return map;
  }
}
