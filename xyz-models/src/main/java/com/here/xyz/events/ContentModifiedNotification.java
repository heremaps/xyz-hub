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

}
