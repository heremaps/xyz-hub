/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

public class SelectiveEvent<SELF extends SelectiveEvent<SELF>> extends ContextAwareEvent<SELF> {

  private List<String> selection;
  private boolean force2D;

  private String ref;

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
  public SELF withSelection(List<String> selection) {
    setSelection(selection);
    return (SELF) this;
  }

  public String getRef() {
    return ref;
  }

  public void setRef(String ref) {
    this.ref = ref;
  }

  public SELF withRef(String ref) {
    setRef(ref);
    return (SELF) this;
  }

  public String getAuthor() {
    return author;
  }

  public void setAuthor(String author) {
    this.author = author;
  }

  public SELF withAuthor(String author) {
    setAuthor(author);
    return (SELF) this;
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
  public SELF withForce2D(boolean force2D) {
    setForce2D(force2D);
    //noinspection unchecked
    return (SELF) this;
  }
}
