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
package com.here.naksha.lib.core.models.geojson.implementation.namespaces;

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.storage.XyzCodec;
import java.util.ArrayList;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/**
 * Tags are special labels that can be added to features managed by Naksha. They are indexed and can be searched for. Actually they are
 * designed to prevent mistakes coming from normal string handling. Language is very complicated and often context related. For example, in
 * most languages {@code I} is lower-cased to {@code i}, but not in all languages. For example in Turkey the {@code I} is lower-cased to
 * {@code ı}. Therefore, lower-casing is not possible without knowing the locale. However, this is a storage system that should be language
 * agnostic, therefore this raises a problem.
 * <p>
 * <h3>General rules</h3> For this purpose tags where made. They operate in three general modes:
 * <ul>
 *   <li>{@code @:} - Full UNICODE, case-sensitive, compaction enabled.
 *   <li>{@code @} - Full UNICODE, case-sensitive, compaction disabled.
 *   <li>{@code ~:} - ASCII only, case-sensitive, compaction enabled.
 *   <li>{@code ~} - ASCII only, case-sensitive, compaction disabled.
 *   <li>{@code :} - ASCII only, case-insensitive (lower cased), compaction enabled.
 *   <li>All other tags are ASCII only, case-insensitive (lower cased), compaction disabled.
 * </ul>
 * The tag need to be prefixed by one of the above characters to enable the corresponding mode. If the first character is none of the
 * above-mentioned ones, the default mode applies, which is ASCII only, case-insensitive, compaction disabled. Due to some downward
 * compatibility with the original Data-Hub and earlier GeoSpace-API there are some additional "hacks" supported, but you should only stick
 * to the above-mentioned general modes.
 * <p>
 * <h3>Normalization</h3>
 * When the full UNICODE mode is selected, only NFKC normalization is done (UNICODE Compatibility decomposition, followed by canonical
 * composition). That means, all UNICODE characters are kept intact in a composite form.
 *
 * <p>When the ASCII only mode is selected, then an NFD normalization (Canonical decomposition) is applied and all characters that are
 * not part of the ASCII table between (inclusive) 32 (space) and (exclusive) 128 are simply removed from the string. So, all characters
 * less than UNICODE 32 or bigger than UNICODE 127 are removed. This limits all allowed characters back to ASCII, for which lower-casing is
 * a defined operation in a unique way without any locale rules.
 *
 * <p>If case-insensitive is selected, then the characters are lower-cased following the ASCII rules.
 * <p><h3>Compaction</h3>
 * All tags are by default de-duplicated, that means tags being found at earlier positions are overridden by those with the same value
 * coming later. For a tag that has the compaction mode enabled, a special rule is applied. For de-duplication, only all characters up until
 * the first found colon sign ({@code :}) are taken into consideration. Apart from that, all character that are used as key are always
 * stored in ASCII only, case-insensitive way. Therefore, the tags array {@code ["@:Foo:bar","test", "@:foO:World"]}
 * will be de-duplicated to {@code ["test","@:foo:World"]}.
 *
 * <p>Actually this behavior is a JAVA optimization and informal agreement. In the database itself, the tags are just a simple array
 * that allow duplicates, with order being significant and no other tweaks like lower-casing. We may implement this logic at some later
 * time as well in the database triggers too, but right now this is not the case, therefore, you may want to invoke compact ones after
 * reading the values from the database and before writing it into the database (the {@link XyzCodec} does this automatically when
 * decoding. This object will remember if it was compacted, so invoking the method multiple times does not have any performance impact.
 * Beware that you can disable auto-compaction, then you have no guarantee the modification keeps this array in the desired state.
 *
 * <p><h3>Compatibility Hacks</h3>
 * As mentioned, there are some hacks to supported downward compatibility. These are, that all tags starting with
 * <pre>{@code ref_<tag>, sourceId_<tag> or #<tag>}</pre> are using the ASCII only, case-sensitive mode (normally indicated by the tilde
 * prefix {@code ~}).
 */
@AvailableSince(NakshaVersion.v2_0_7)
public class XyzTags extends ArrayList<String> {
  // TODO: We need to move the normalization code, key/value split and compaction code here!
}
