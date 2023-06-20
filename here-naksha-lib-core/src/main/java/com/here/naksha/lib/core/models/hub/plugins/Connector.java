/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

package com.here.naksha.lib.core.models.hub.plugins;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.INaksha;
import java.lang.reflect.InvocationTargetException;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/** A connector is a special {@link EventHandler} which “connects” to a storage. */
@SuppressWarnings("unused")
@JsonTypeName(value = "Connector")
@AvailableSince(INaksha.v2_0)
public final class Connector extends EventHandler {

    @AvailableSince(INaksha.v2_0)
    public static final String STORAGE_ID = "storageId";

    /**
     * Create a new connector.
     *
     * @param id the identifier of the connector.
     * @param cla$$ the class, that implements the event handler.
     * @param storage the storage to which to attach the connector.
     */
    @AvailableSince(INaksha.v2_0)
    public Connector(@NotNull String id, @NotNull Class<? extends IEventHandler> cla$$, @NotNull Storage storage) {
        super(id, cla$$);
        this.storageId = storage.getId();
    }

    /**
     * Create a new connector.
     *
     * @param id the identifier of the connector.
     * @param className the full qualified name of the class to load.
     * @param storageId the identifier of the storage to use.
     */
    @JsonCreator
    @AvailableSince(INaksha.v2_0)
    public Connector(
            @JsonProperty(ID) @NotNull String id,
            @JsonProperty(CLASS_NAME) @NotNull String className,
            @JsonProperty(STORAGE_ID) @NotNull String storageId) {
        super(id, className);
        this.storageId = storageId;
    }

    /** If the identifier of the storage this event-handler should use. */
    @AvailableSince(INaksha.v2_0)
    @JsonProperty(STORAGE_ID)
    public @NotNull String storageId;

    @Override
    public @NotNull IEventHandler newInstance() throws Exception {
        try {
            return PluginCache.newInstance(className, IEventHandler.class, this);
        } catch (InvocationTargetException ite) {
            if (ite.getCause() instanceof Exception e) {
                throw e;
            }
            throw ite;
        }
    }
}
