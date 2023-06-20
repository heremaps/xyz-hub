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

package com.here.naksha.lib.core.models.payload.events;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.here.naksha.lib.core.models.payload.Event;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class FeatureEvent extends Event {

    /**
     * An optional list of fields to be returned (basically, a white list). If {@code null} or empty,
     * then full features are requested.
     *
     * @since 0.6.0
     */
    @JsonInclude(Include.NON_EMPTY)
    private @Nullable List<@NotNull String> selection;

    /**
     * If 2D coordinates should be enforced.
     *
     * @since 0.6.0
     */
    @JsonInclude(Include.NON_DEFAULT)
    private boolean force2D;

    private String ref;

    private String author;

    /**
     * The selection of properties to return.
     *
     * @return The list of properties to return; {@code null} if all properties should be returned.
     */
    public @Nullable List<@NotNull String> getSelection() {
        return this.selection;
    }

    public void setSelection(@Nullable List<@NotNull String> selection) {
        this.selection = selection;
    }

    public String getRef() {
        return ref;
    }

    public void setRef(String ref) {
        this.ref = ref;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    @SuppressWarnings("WeakerAccess")
    public boolean isForce2D() {
        return force2D;
    }

    @SuppressWarnings("WeakerAccess")
    public void setForce2D(boolean force2D) {
        this.force2D = force2D;
    }
}
