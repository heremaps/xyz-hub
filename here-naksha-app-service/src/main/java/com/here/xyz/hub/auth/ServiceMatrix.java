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
package com.here.xyz.hub.auth;

import java.util.LinkedHashMap;

/**
 * The service action matrix contains the action matrices of multiple services. The key is the service identifier and the value the
 * corresponding action matrix that describes all access rights the corresponding user or application does have for that service. This
 * structure is very helpful for caching and offline access control.
 */
public class ServiceMatrix extends LinkedHashMap<String, ActionMatrix> {}
