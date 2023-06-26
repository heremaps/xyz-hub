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
package com.here.naksha.lib.core.models.payload.events.clustering;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.naksha.lib.core.models.Typed;

/**
 * The clustering algorithm to apply to the data within the result.
 *
 * <p>If selected, returning the data in a clustered way. This means the data is not necessarily
 * returned in its original shape or with its original properties. Depending on the chosen
 * clustering algorithm, there could be different mandatory and/or optional parameters to specify
 * the behavior of the algorithm.
 */
@JsonSubTypes({@JsonSubTypes.Type(value = ClusteringQuadBin.class), @JsonSubTypes.Type(value = ClusteringHexBin.class)})
public abstract class Clustering implements Typed {}
