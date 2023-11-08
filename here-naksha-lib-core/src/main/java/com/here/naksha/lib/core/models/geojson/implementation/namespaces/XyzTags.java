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
package com.here.naksha.lib.core.models.geojson.implementation.namespaces;

import com.here.naksha.lib.core.NakshaVersion;
import java.util.ArrayList;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/**
 * Tags are special labels that can be added to features managed by Naksha. They are indexed and can be searched for. Actually they are
 * designed to prevent mistakes coming from normal string handling. Language is very complicated and often context related. For example, in
 * most languages {@code I} is lower-cased to {@code i}, but not in all languages. For example in Turkey the {@code I} is lower-cased to
 * {@code ı}. Therefore, lower-casing is not possible without knowing the locale. However, this is a storage system that should be language
 * agnostic, therefore this raises a problem.
 *
 * <p>For this purpose tags where made. They (by default) only allow latin letters, digits, colon underscore and minus. They are event, by
 * default, split by an equal sign into a key and a value part and those with the same key will override each other. This operation is
 * called compaction. When compaction is executed it will remove duplicates (by their keys) and sort all tags alphabetically to support
 * auto-merge algorithms in their work.
 *
 * <p>Sadly, sometimes it is required to add tags that do not fulfill these requirements, for example when unique identifiers are used as
 * tags, lower-casing them may turn them useless, because the identifier may be case-sensitive. There are other examples that add the need
 * to support case-sensitive tags. For these cases, by definition, all tags that start with the prefix {@code ref_} and {@code #} are not
 * lower-cased, nor are they split at the equals sign.
 *
 * <p>Using the special separator {@code :=} prevents case changes, but applies the split and merge algorithm, so {@code Is:=Now} will stay
 * as it is, but adding another tag later being {@code Is:=bar} will replace the first tag.
 *
 * <p>Actually this behavior is a server-side optimization and informal agreement. In the database itself, the tags are just a simple array
 * that allows duplicates, with order being significant and other no tweaks like lower-casing or split and merge. We may implement this
 * logic at some later time as well in the database triggers too, but right now this is not the case, therefore, you should invoke compact
 * ones after reading the values from the database. This object will remember if it was compacted, so invoking the method multiple times
 * does not have any impact. Beware that you can disable auto-compaction, then you have no guarantee the modification keeps this array in
 * the desired state.
 */
@AvailableSince(NakshaVersion.v2_0_7)
public class XyzTags extends ArrayList<String> {
  // TODO: We need to move the normalization code, key/value split and compaction code here!
}
