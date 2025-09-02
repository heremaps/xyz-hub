/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.hub.util;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.DumperOptions;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public final class OpenApiTestUtil {
    private OpenApiTestUtil() {}

    public static String sanitizeAndMaterialize(String pathOrClasspathResource) {
        try (InputStream in = resolve(pathOrClasspathResource)) {
            if (in == null) throw new FileNotFoundException("OpenAPI not found: " + pathOrClasspathResource);

            Yaml yaml = new Yaml();
            Object doc = yaml.load(in);
            stripAllowReserved(doc);

            DumperOptions opts = new DumperOptions();
            opts.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
            opts.setPrettyFlow(true);
            Yaml outYaml = new Yaml(opts);

            Path tmp = Files.createTempFile("openapi-vertx-", ".yaml");
            try (Writer w = Files.newBufferedWriter(tmp)) {
                outYaml.dump(doc, w);
            }
            return tmp.toAbsolutePath().toString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static InputStream resolve(String path) throws IOException {
        Path p = Paths.get(path);
        if (Files.exists(p)) return Files.newInputStream(p);

        p = Paths.get(System.getProperty("user.dir")).resolve(path).normalize();
        if (Files.exists(p)) return Files.newInputStream(p);

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream cp = cl.getResourceAsStream(path.startsWith("/") ? path.substring(1) : path);
        if (cp != null) return cp;

        String fileOnly = Paths.get(path).getFileName().toString();
        return cl.getResourceAsStream(fileOnly);
    }

    @SuppressWarnings("unchecked")
    private static void stripAllowReserved(Object node) {
        if (node instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) node;
            map.remove("allowReserved");
            for (Object v : new ArrayList<>(map.values())) stripAllowReserved(v);
        } else if (node instanceof List) {
            for (Object v : (List<?>) node) stripAllowReserved(v);
        }
    }

    @SuppressWarnings("unchecked")
    public static Set<String> readSecuritySchemeNames(String pathOrClasspathResource) {
        try (InputStream in = resolve(pathOrClasspathResource)) {
            if (in == null) return Collections.emptySet();
            Yaml yaml = new Yaml();
            Object doc = yaml.load(in);
            if (!(doc instanceof Map)) return Collections.emptySet();
            Map<String, Object> root = (Map<String, Object>) doc;
            Object components = root.get("components");
            if (!(components instanceof Map)) return Collections.emptySet();
            Object schemes = ((Map<String, Object>) components).get("securitySchemes");
            if (!(schemes instanceof Map)) return Collections.emptySet();
            return new LinkedHashSet<>(((Map<String, Object>) schemes).keySet().stream()
                    .map(Object::toString).toList());
        } catch (IOException e) {
            return Collections.emptySet();
        }
    }
}

