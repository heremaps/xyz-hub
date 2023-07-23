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

import com.here.naksha.lib.core.NakshaVersion;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/** All serialization views. */
@AvailableSince(NakshaVersion.v2_0_3)
@SuppressWarnings("unused")
public interface ViewSerialize {

  /** Serialize for the normal users. */
  @AvailableSince(NakshaVersion.v2_0_3)
  interface User extends ViewSerialize, ViewMember.Export.User {}

  /** Serialize for managers. */
  @AvailableSince(NakshaVersion.v2_0_3)
  interface Manager extends ViewSerialize, ViewMember.Export.User, ViewMember.Export.Manager {}

  /** Serialize internally. */
  @AvailableSince(NakshaVersion.v2_0_3)
  interface Internal
      extends ViewSerialize, ViewMember.Export.User, ViewMember.Export.Manager, ViewMember.Export.Internal {}

  /** Serialized for storage only. */
  @AvailableSince(NakshaVersion.v2_0_3)
  interface Storage
      extends ViewSerialize,
          ViewMember.Export.User,
          ViewMember.Export.Manager,
          ViewMember.Export.Internal,
          ViewMember.Storage {}

  /** Serialized for hashing only. */
  @AvailableSince(NakshaVersion.v2_0_3)
  interface Hash extends ViewSerialize, ViewMember.Hashing {}

  /** Serialize all members with no exception. */
  @AvailableSince(NakshaVersion.v2_0_3)
  interface All
      extends ViewSerialize,
          ViewMember.Export.User,
          ViewMember.Export.Manager,
          ViewMember.Export.Internal,
          ViewMember.Storage,
          ViewMember.Hashing {}
}
