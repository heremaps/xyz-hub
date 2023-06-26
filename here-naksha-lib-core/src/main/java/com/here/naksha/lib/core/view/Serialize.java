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
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/** All serialization views. */
@AvailableSince(INaksha.v2_0_3)
@SuppressWarnings("unused")
public interface Serialize {

  /** Serialize for the normal users. */
  @AvailableSince(INaksha.v2_0_3)
  interface User extends Serialize, Member.Export.User {}

  /** Serialize for managers. */
  @AvailableSince(INaksha.v2_0_3)
  interface Manager extends Serialize, Member.Export.User, Member.Export.Manager {}

  /** Serialize internally. */
  @AvailableSince(INaksha.v2_0_3)
  interface Internal extends Serialize, Member.Export.User, Member.Export.Manager, Member.Export.Internal {}

  /** Serialized for storage only. */
  @AvailableSince(INaksha.v2_0_3)
  interface Storage extends Serialize, Member.Storage {}

  /** Serialized for hashing only. */
  @AvailableSince(INaksha.v2_0_3)
  interface Hash extends Serialize, Member.Hashing {}

  /** Serialize all members with no exception. */
  @AvailableSince(INaksha.v2_0_3)
  interface All
      extends Serialize,
          Member.Export.User,
          Member.Export.Manager,
          Member.Export.Internal,
          Member.Storage,
          Member.Hashing {}
}
