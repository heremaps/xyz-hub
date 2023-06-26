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
package com.here.naksha.lib.core.view;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.view.ViewMember.Import;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/** All deserialization views. */
@AvailableSince(INaksha.v2_0_3)
@SuppressWarnings("unused")
public interface ViewDeserialize {

  /** Deserialized input from a user. */
  @AvailableSince(INaksha.v2_0_3)
  interface User extends ViewDeserialize, Import.User {}

  /** Deserialize input from a manager. */
  @AvailableSince(INaksha.v2_0_3)
  interface Manager extends ViewDeserialize, Import.User, Import.Manager {}

  /** Deserialize input from an internal component. */
  @AvailableSince(INaksha.v2_0_3)
  interface Internal extends ViewDeserialize, Import.User, Import.Manager, Import.Internal {}

  /** Deserialize from the database. */
  @AvailableSince(INaksha.v2_0_3)
  interface Storage extends ViewDeserialize, ViewMember.Storage {}

  /** Deserialize all members, even those normally only for hashing purpose. */
  @AvailableSince(INaksha.v2_0_3)
  interface All
      extends ViewDeserialize,
          Import.User,
          Import.Manager,
          Import.Internal,
          ViewMember.Storage,
          ViewMember.Hashing {}
}
