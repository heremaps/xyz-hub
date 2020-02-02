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

package com.here.xyz.hub.task;

import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.task.ModifyOp.Entry;
import com.here.xyz.hub.util.diff.Difference;
import com.here.xyz.hub.util.diff.Patcher;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A modify operation
 */
public abstract class ModifyOp<T, K extends Entry<T>> {

  public final List<K> entries;
  public final IfExists ifExists;
  public final IfNotExists ifNotExists;
  public final boolean isTransactional;

  public ModifyOp(List<K> entries, IfNotExists ifNotExists, IfExists ifExists, boolean isTransactional) {
    this.ifExists = ifExists;
    this.ifNotExists = ifNotExists;
    this.isTransactional = isTransactional;
    this.entries = entries;
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
        // IF NOT EXISTS
        if (entry.head == null) {
          switch (ifNotExists) {
            case RETAIN:
              entry.result = null;
              break;
            case CREATE:
              entry.result = entry.create();
              break;
            case ERROR:
              throw new ModifyOpError("The record does not exist.");
          }
        }
        // IF EXISTS
        else {
          switch (ifExists) {
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
              throw new ModifyOpError("The record exists.");
          }
        }

        // Check if the isModified flag is not already set. Compare the objects in case it is not set yet.
        entry.isModified = entry.isModified || entry.isModified();
      } catch (ModifyOpError e) {
        if (isTransactional) {
          throw e;
        }
        // TODO: Check if this is included in the failed array
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

    public boolean isModified;
    /**
     * The input as it comes from the caller
     */
    public Map<String, Object> input;

    /**
     * The latest state the object currently has in the data storage
     */
    public T head;

    private Map<String, Object> headMap;

    private Map<String, Object> getHeadMap() throws HttpException, ModifyOpError {
      if( headMap == null && head != null){
          headMap = toMap(head);
      }
      return headMap;
    }

    /**
     * The state onto which the caller made the modifications
     */
    public T base;

    /**
     * The resulting target state which should go to the data storage after merging
     */
    public T result;

    private Map<String, Object> resultMap;

    private Map<String, Object> getResultMap() throws HttpException, ModifyOpError {
      if( resultMap == null && result!=null){
        resultMap = toMap(result);
      }
      return resultMap;
    };

    public Exception exception;
    public String inputUUID;

    public Entry(Map<String, Object> input) {
      this.inputUUID = getUuid(input);
      this.input = filterMetadata(input);
    }

    protected abstract String getId(T record);

    protected abstract String getUuid(T record);

    protected abstract String getUuid(Map<String, Object> record);

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
        final Difference mergedDiff = Patcher.mergeDifferences(diffInput, diffHead);
        Patcher.patch(resultMap, mergedDiff);
        this.resultMap = resultMap;
        return fromMap(resultMap);
      } catch (Exception e) {
        throw new ModifyOpError(e.getMessage());
      }
    }

    public T replace() throws ModifyOpError, HttpException {
      if (inputUUID != null && !com.google.common.base.Objects.equal(inputUUID, getUuid(head))) {
        throw new ModifyOpError(
            "The feature with id " + getId(head) + " cannot be replaced. The provided UUID doesn't match the UUID of the head state: "
                + getUuid(head));
      }
      return fromMap(input);
    }

    public T delete() throws ModifyOpError, HttpException {
      if (inputUUID != null && !com.google.common.base.Objects.equal(inputUUID, getUuid(head))) {
        throw new ModifyOpError(
            "The feature with id " + getId(head) + " cannot be replaced. The provided UUID doesn't match the UUID of the head state: "
                + getUuid(head));
      }
      if (head != null) {
        isModified = true;
      }
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
     * Checks, if the result of the operation is different than the the head state.
     *
     * @return true, if the result of the operation is different than the the head state.
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
   * Filter recursively all properties from the provided filter, where the value is set to 'true'.
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