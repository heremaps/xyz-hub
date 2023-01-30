/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.xyz.hub.util.diff;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * All differences extend this base difference class.
 */
public interface Difference {

  /**
   * A common interface for primitive changes.
   */
  abstract class Primitive implements Difference {

    private Object newValue;
    private Object oldValue;

    public Primitive(Object oldValue, Object newValue) {
      this.oldValue = oldValue;
      this.newValue = newValue;
    }

    /**
     * Returns the old value.
     *
     * @return the old value.
     */
    public Object oldValue() {
      return oldValue;
    }

    /**
     * Returns the new value.
     *
     * @return the new value.
     */
    public Object newValue() {
      return newValue;
    }

    @Override
    public String toString() {
      return "" + newValue;
    }
  }


  /**
   * Represents a difference between maps.
   */
  class DiffMap extends HashMap<Object, Difference> implements Difference {

  }

  /**
   * Represents a difference between two lists.
   */
  class DiffList extends ArrayList<Difference> implements Difference {

    int originalLength;
    int newLength;

    DiffList(final int totalLength) {
      super(totalLength);
    }
  }

  /**
   * An insert.
   */
  class Insert extends Primitive {

    Insert(final Object value) {
      super(null, value);
    }
  }

  /**
   * A remove.
   */
  class Remove extends Primitive {

    Remove(final Object value) {
      super(value, null);
    }

  }

  /**
   * An update.
   */
  class Update extends Primitive {

    Update(Object oldValue, Object newValue) {
      super(oldValue, newValue);
    }
  }
}
