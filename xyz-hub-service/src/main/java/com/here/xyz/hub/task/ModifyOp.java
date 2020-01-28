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
import java.util.stream.Collectors;

/**
 * A modify operation, which transforms an INPUT object to a TARGET object.
 *
 * @param <INPUT> some input, which may be a full state or just some instructions how to modify the current source state to produce a
 * certain target state.
 * @param <SOURCE> the type current state.
 * @param <TARGET> the target state that is the source state modified by the operation described by the input.
 */
public abstract class ModifyOp<INPUT, SOURCE, TARGET> {

  public final List<Entry<INPUT, SOURCE, TARGET>> entries;
  public final IfExists ifExists;
  public final IfNotExists ifNotExists;
  public final boolean isTransactional;

  public ModifyOp(List<INPUT> inputStates, IfNotExists ifNotExists, IfExists ifExists, boolean isTransactional) {
    this.ifExists = ifExists;
    this.ifNotExists = ifNotExists;
    this.isTransactional = isTransactional;
    this.entries = (inputStates == null) ? Collections.emptyList()
        : inputStates.stream().map(Entry<INPUT, SOURCE, TARGET>::new).collect(Collectors.toList());
  }

  /**
   * Applies the modifications provided by the input to the source state, both provided with the input, and produces the target state as
   * output.
   *
   * @throws ModifyOpError when a processing error occurs.
   */
  public void process() throws ModifyOpError, HttpException {
    for (Entry<INPUT, SOURCE, TARGET> entry : entries) {
      try {
        // IF NOT EXISTS
        if (entry.head == null) {
          switch (ifNotExists) {
            case RETAIN:
              entry.result = null;
              break;
            case CREATE:
              entry.result = create(entry.input);
              break;
            case ERROR:
              throw new ModifyOpError("The record does not exist.");
          }
        }
        // IF EXISTS
        else {
          switch (ifExists) {
            case RETAIN:
              entry.result = transform(entry.head);
              break;
            case MERGE:
              entry.result = merge(entry.head, entry.base, entry.input);
              break;
            case PATCH:
              entry.result = patch(entry.head, entry.base, entry.input);
              break;
            case REPLACE:
              entry.result = replace(entry.head, entry.input);
              break;
            case DELETE:
              entry.result = null;
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

  public abstract TARGET patch(SOURCE headState, SOURCE baseState, INPUT inputState) throws ModifyOpError, HttpException;

  public abstract TARGET merge(SOURCE headState, SOURCE baseState, INPUT inputState) throws ModifyOpError, HttpException;

  public abstract TARGET replace(SOURCE headState, INPUT inputState) throws ModifyOpError, HttpException;

  public abstract TARGET create(INPUT inputState) throws ModifyOpError, HttpException;

  public abstract TARGET transform(SOURCE sourceState) throws ModifyOpError, HttpException;

  /**
   * Returns true, if the 2 objects are equal, apart from any metadata information added by the service, such as timestamps, uuid, etc.
   *
   * @param state1 the source state.
   * @param state2 the target state.
   * @return true when the source and target state are logically equal; false otherwise.
   */
  public abstract boolean dataEquals(SOURCE state1, TARGET state2);

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

  public static class Entry<INPUT, SOURCE, TARGET> {

    public boolean isModified;
    /**
     * The input as it comes from the caller
     */
    public INPUT input;

    /**
     * The latest state the object currently has in the data storage
     */
    public SOURCE head;

    /**
     * The state onto which the caller made the modifications
     */
    public SOURCE base;

    /**
     * The resulting target state which should go to the data storage after merging
     */
    public TARGET result;

    public Exception exception;

    public Entry(INPUT input) {
      this.input = input;
    }
  }

  public static class ModifyOpError extends Exception {

    public ModifyOpError(String msg) {
      super(msg);
    }
  }
}
