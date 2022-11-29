/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.CharStreams;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.Service;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class JwtGenerator {

  private static JWTAuth authProvider;
  private static JWTOptions jwtOptions = new JWTOptions().setAlgorithm("RS256");

  static {
    try {
      setup();
    } catch (IOException e) {
      throw new RuntimeException("Error when loading JWT key pair.", e);
    }
  }

  /*
  Steps to generate a valid key-pair for JWT generation:

    1. Generate a private key with the command:
      openssl genpkey -out jwt.key -algorithm RSA -pkeyopt rsa_keygen_bits:2048
    2. Generate the according public key with the command:
      openssl rsa -in jwt.key -pubout -outform PEM -out jwt.pub
    3. In case you want to generate a jwt token using jwt.io, transform your private key into PEM first, by running:
      openssl rsa -in jwt.key -outform pem -out filekey.pem

  */

  private static void setup() throws IOException {
    final byte[] bytes = Core.readFileFromHomeOrResource("/auth/jwt.key");
    JWTAuthOptions authConfig = new JWTAuthOptions()
        .setJWTOptions(jwtOptions)
        .addPubSecKey(new PubSecKeyOptions()
            .setAlgorithm("RS256")
            .setBuffer(new String(bytes, StandardCharsets.UTF_8)));

    authProvider = JWTAuth.create(Service.vertx, authConfig);
  }

  private static String readResourceFile(String resourceFilename) throws IOException {
    try (Reader r = new InputStreamReader(JwtGenerator.class.getResourceAsStream(resourceFilename))) {
      return CharStreams.toString(r).trim();
    }
  }

  public static String generateToken(JWTPayload payload) {
    return authProvider.generateToken(
        new JsonObject(DatabindCodec.mapper().convertValue(payload, new TypeReference<Map<String, Object>>() {})), jwtOptions);
  }

  public static String generateToken(String resourceFilename) {
    return generateToken(readTokenPayload(resourceFilename));
  }

  public static JWTPayload readTokenPayload(String resourceFilename) {
    try {
      return Json.decodeValue(readResourceFile(resourceFilename), JWTPayload.class);
    } catch (IOException e) {
      throw new RuntimeException("Error while reading token from resource file: " + resourceFilename, e);
    }
  }

}
