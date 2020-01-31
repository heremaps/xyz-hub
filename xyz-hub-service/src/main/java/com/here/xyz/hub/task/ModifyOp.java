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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A modify operation
 *
 * @param <T> the record type
 */
public abstract class ModifyOp<T> {

  public final List<Entry<T>> entries;
  public final IfExists ifExists;
  public final IfNotExists ifNotExists;
  public final boolean isTransactional;

  public ModifyOp(List<Map<String,Object>> inputStates, IfNotExists ifNotExists, IfExists ifExists, boolean isTransactional) {
    this.ifExists = ifExists;
    this.ifNotExists = ifNotExists;
    this.isTransactional = isTransactional;
    this.entries = (inputStates == null) ? Collections.emptyList()
        : inputStates.stream().map(Entry<T>::new).collect(Collectors.toList());
  }

  /**
   * Applies the modifications provided by the input to the source state, both provided with the input, and produces the target state as
   * output.
   *
   * @throws ModifyOpError when a processing error occurs.
   */
  public void process() throws ModifyOpError, HttpException {
    for (Entry<T> entry : entries) {
      try {
        // IF NOT EXISTS
        if (entry.head == null) {
          switch (ifNotExists) {
            case RETAIN:
              entry.result = null;
              break;
            case CREATE:
              entry.result = create(entry, entry.input);
              break;
            case ERROR:
              throw new ModifyOpError("The record does not exist.");
          }
        }
        // IF EXISTS
        else {
          switch (ifExists) {
            case RETAIN:
              entry.result = transform(entry, entry.head);
              break;
            case MERGE:
              entry.result = merge(entry, entry.head, entry.base, entry.input);
              break;
            case PATCH:
              entry.result = patch(entry, entry.head, entry.base, entry.input);
              break;
            case REPLACE:
              entry.result = replace(entry, entry.head, entry.input);
              break;
            case DELETE:
              entry.result = delete(entry, entry.head, entry.input);
              break;
            case ERROR:
              throw new ModifyOpError("The record exists.");
          }
        }

        // Check if the isModified flag is not already set. Compare the objects in case it is not set yet.
        entry.isModified = entry.isModified || !dataEquals(entry.head, entry.result);
      } catch (ModifyOpError e) {
        if (isTransactional) {
          throw e;
        }
        // TODO: Check if this is included in the failed array
        entry.exception = e;
      }
    }
  }

  public abstract T patch(Entry<T> entry, T headState, T baseState, Map<String,Object> inputState) throws ModifyOpError, HttpException;

  public abstract T merge(Entry<T> entry, T headState, T baseState, Map<String,Object> inputState) throws ModifyOpError, HttpException;

  public abstract T replace(Entry<T> entry, T headState, Map<String,Object> inputState) throws ModifyOpError, HttpException;

  public abstract T delete(Entry<T> entry, T headState, Map<String,Object> inputState) throws ModifyOpError, HttpException;

  public abstract T create(Entry<T> entry, Map<String,Object> inputState) throws ModifyOpError, HttpException;

  public abstract T transform(Entry<T> entry, T sourceState) throws ModifyOpError, HttpException;

  /**
   * Returns true, if the 2 objects are equal, apart from any metadata information added by the service, such as timestamps, uuid, etc.
   *
   * @param state1 the source state.
   * @param state2 the target state.
   * @return true when the source and target state are logically equal; false otherwise.
   */
  public abstract boolean dataEquals(T state1, T state2);

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

  public static class Entry<T> {

    public boolean isModified;
    /**
     * The input as it comes from the caller
     */
    public Map<String,Object> input;

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

    public Exception exception;
    public String inputId;
    public String inputUUID;

    public Entry(Map<String,Object> input) {
      this.input = input;
    }
  }

  public static class ModifyOpError extends Exception {

    public ModifyOpError(String msg) {
      super(msg);
    }
  }
}
