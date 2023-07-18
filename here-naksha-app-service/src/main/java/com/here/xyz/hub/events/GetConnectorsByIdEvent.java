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
package com.here.xyz.hub.events;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class GetConnectorsByIdEvent extends AbstractConnectorEvent {

  public GetConnectorsByIdEvent() {
    this.ids = new ArrayList<>();
  }

  public @NotNull List<@NotNull String> ids;

  public @NotNull GetConnectorsByIdEvent add(@NotNull String id) {
    ids.add(id);
    return this;
  }

  public @NotNull GetConnectorsByIdEvent addAll(@NotNull String... id) {
    Collections.addAll(this.ids, id);
    return this;
  }
}
