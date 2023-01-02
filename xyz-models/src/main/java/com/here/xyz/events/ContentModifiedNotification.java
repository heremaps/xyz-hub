/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

/**
 * Will be sent by the XYZ Hub to indicate that the content of a space has changed.
 * After a change of the space's content the event will be sent asynchronously.
 * It might happen that only one {@link ContentModifiedNotification} event is sent for multiple changes of the space's content.
 * There is neither a guarantee about how often the {@link ContentModifiedNotification} event will be sent nor about the time-interval
 * between those events.
 */
public class ContentModifiedNotification extends Event<ContentModifiedNotification> {

  /**
   * The latest version of the space contents as it has been seen on the service-node which sent this {@link ContentModifiedNotification}.
   * There is neither a guarantee that the version value is pointing to the actual latest version of the space's content nor there is a
   * guarantee that it's defined at all.
   * If the value exists and it points to a value > 0, that is the version of the latest write to that space as it has been performed by
   * the triggering service-node.
   */
  private long spaceVersion;

  /**
   * @return The latest space-version as it has been seen on the service-node which sent this {@link ContentModifiedNotification}.
   */
  public long getSpaceVersion() {
    return spaceVersion;
  }

  public void setSpaceVersion(long spaceVersion) {
    this.spaceVersion = spaceVersion;
  }

  public ContentModifiedNotification withSpaceVersion(long spaceVersion) {
    setSpaceVersion(spaceVersion);
    return this;
  }

}
