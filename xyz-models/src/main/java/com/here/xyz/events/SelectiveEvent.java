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

package com.here.xyz.events;

import java.util.List;

public class SelectiveEvent<T extends SelectiveEvent> extends ContextAwareEvent<T> {

  private List<String> selection;
  private boolean force2D;
  private String ref;
  private long minVersion;
  private String author;

  @SuppressWarnings("unused")
  public List<String> getSelection() {
    return this.selection;
  }

  @SuppressWarnings("WeakerAccess")
  public void setSelection(List<String> selection) {
    this.selection = selection;
  }

  @SuppressWarnings("unused")
  public T withSelection(List<String> selection) {
    setSelection(selection);
    return (T) this;
  }

  public String getRef() {
    return ref;
  }

  public void setRef(String ref) {
    this.ref = ref;
  }

  public T withRef(String ref) {
    setRef(ref);
    return (T) this;
  }

  public static class Ref {
    public static final String HEAD = "HEAD";
    public static final String ALL_VERSIONS = "*";
    private long version = -1;
    private boolean head;
    private boolean allVersions;

    public Ref(String ref) {
      if (ref == null || ref.isEmpty() || HEAD.equals(ref))
        head = true;
      else if (ALL_VERSIONS.equals(ref))
        allVersions = true;
      else
        try {
          version = Long.parseLong(ref);
        }
        catch (NumberFormatException e) {
          throw new InvalidRef("Invalid ref: the provided ref is not a valid ref or version: \"" + ref + "\"");
        }
    }

    public boolean isHead() {
      return head;
    }

    public boolean isAllVersions() {
      return allVersions;
    }

    public boolean isSingleVersion() {
      return !isAllVersions();
    }

    @Override
    public String toString() {
      if (version < 0 && !head)
        throw new IllegalArgumentException("Not a valid ref");
      if (head)
        return HEAD;
      return String.valueOf(version);
    }

    public static class InvalidRef extends IllegalArgumentException {
      private InvalidRef(String message) {
        super(message);
      }
    }
  }

  public long getMinVersion() {
    return minVersion;
  }

  public void setMinVersion(long minVersion) {
    this.minVersion = minVersion;
  }

  public T withMinVersion(long minVersion) {
    setMinVersion(minVersion);
    return (T) this;
  }

  public String getAuthor() {
    return author;
  }

  public void setAuthor(String author) {
    this.author = author;
  }

  public T withAuthor(String author) {
    setAuthor(author);
    return (T) this;
  }

  @SuppressWarnings("WeakerAccess")
  public boolean isForce2D() {
    return force2D;
  }

  @SuppressWarnings("WeakerAccess")
  public void setForce2D(boolean force2D) {
    this.force2D = force2D;
  }

  @SuppressWarnings("unused")
  public T withForce2D(boolean force2D) {
    setForce2D(force2D);
    //noinspection unchecked
    return (T) this;
  }
}
