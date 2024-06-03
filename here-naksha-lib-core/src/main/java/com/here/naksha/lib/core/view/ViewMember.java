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
package com.here.naksha.lib.core.view;

// See: https://www.baeldung.com/jackson-json-view-annotation

import com.fasterxml.jackson.annotation.JsonView;
import com.here.naksha.lib.core.NakshaVersion;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/**
 * Members that are not annotated for special visibility conditions, will always be part of the serialization and deserialization. When
 * members annotated with the {@link JsonView} annotation, then they have some distinct capabilities. How they are exported to clients or
 * how they are imported from clients, and if they are persisted into a storage.
 *
 * <p><b>Note</b>: Control can be fine-grained, so to make a virtual read-only property, that can only be read by clients, but does not
 * exist in the database nor is available otherwise internally, do the following: <pre>{@code
 * @JsonView({Member.Export.User.class, Member.Export.Manager.class})
 * @JsonGetter("foo")
 * public @NotNull String foo() {
 *   return "foo";
 * }
 * }</pre>
 * <p>
 * This will result in the property being part of the public REST API, but the client can't set it (no import rule), and it is not
 * serialized internally or stored in the database. It is recommended to not add the {@link JsonView} annotation, except some special
 * handling needed. In that case, whenever possible, use the default {@link ViewMember.User}, {@link ViewMember.Manager} or {@link ViewMember.Internal}
 * annotations, which declare import and export rules. Only for special cases do use individual fine-grained annotations.
 */
@SuppressWarnings("unused")
@AvailableSince(NakshaVersion.v2_0_3)
public interface ViewMember {

  /**
   * The member can be read and written by all authenticated users, is stored in the storage and available for hashing.
   */
  @AvailableSince(NakshaVersion.v2_0_3)
  interface User extends Export.User, Import.User, Storage, Hashing {}

  /**
   * The member can only be read and written by managers, is stored in the storage and available for hashing.
   */
  @AvailableSince(NakshaVersion.v2_0_3)
  interface Manager extends Export.Manager, Import.Manager, Storage, Hashing {}

  /**
   * The member can only be read and written by internal trusted components, is stored in the storage and available for hashing.
   */
  @AvailableSince(NakshaVersion.v2_0_3)
  interface Internal extends Export.Internal, Import.Internal, Storage, Hashing {}

  /**
   * Fine-grained serialization control.
   */
  @AvailableSince(NakshaVersion.v2_0_3)
  interface Export {

    /**
     * The member is visible to users.
     */
    @AvailableSince(NakshaVersion.v2_0_3)
    interface User extends Export {}

    /**
     * The member is visible for managers.
     */
    @AvailableSince(NakshaVersion.v2_0_3)
    interface Manager extends Export {}

    /**
     * The member is visible for internal components.
     */
    @AvailableSince(NakshaVersion.v2_0_3)
    interface Internal extends Export {}

    /**
     * The member is visible for users, managers and internally.
     */
    @AvailableSince(NakshaVersion.v2_0_3)
    interface All extends User, Manager, Internal {}
  }

  /**
   * Fine-grained deserialization control.
   */
  @AvailableSince(NakshaVersion.v2_0_3)
  interface Import {

    /**
     * Read the member from user input.
     */
    @AvailableSince(NakshaVersion.v2_0_3)
    interface User extends Import {}

    /**
     * Read the member from manager input.
     */
    @AvailableSince(NakshaVersion.v2_0_3)
    interface Manager extends Import {}

    /**
     * Read the member from internal components.
     */
    @AvailableSince(NakshaVersion.v2_0_3)
    interface Internal extends Import {}

    /**
     * Read the member from users, managers and internal components.
     */
    @AvailableSince(NakshaVersion.v2_0_3)
    interface All extends User, Manager, Internal {}
  }

  /**
   * The member is present in the storage.
   */
  @AvailableSince(NakshaVersion.v2_0_3)
  interface Storage {}

  /**
   * If the member should be part of a serialization done for hashing.
   */
  @AvailableSince(NakshaVersion.v2_0_3)
  interface Hashing {}
}
