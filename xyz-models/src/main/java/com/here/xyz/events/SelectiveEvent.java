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

public class SelectiveEvent<T extends ContextAwareEvent> extends ContextAwareEvent<T> {

  private List<String> selection;

  private long revision = Long.MAX_VALUE; //Default is HEAD revision

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

  public long getRevision() {
    return revision;
  }

  public void setRevision(long revision) {
    this.revision = revision;
  }

  public T withRevision(long revision) {
    setRevision(revision);
    return (T) this;
  }

  public String getAuthor() {
    return author;
  }

  public void setAuthor(String author) {
    this.author = author;
  }

  public T withRevision(String author) {
    setAuthor(author);
    return (T) this;
  }
}
