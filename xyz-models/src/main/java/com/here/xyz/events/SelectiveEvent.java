/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

import static com.here.xyz.models.hub.Ref.HEAD;

import com.here.xyz.models.hub.Ref;
import java.util.List;

public class SelectiveEvent<T extends SelectiveEvent> extends ContextAwareEvent<T> {
  private List<String> selection;
  private boolean force2D;
  private Ref ref = new Ref(HEAD);
  private long minVersion;

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

  public Ref getRef() {
    return ref;
  }

  public void setRef(Ref ref) {
    this.ref = ref;
  }

  public T withRef(Ref ref) {
    setRef(ref);
    return (T) this;
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
