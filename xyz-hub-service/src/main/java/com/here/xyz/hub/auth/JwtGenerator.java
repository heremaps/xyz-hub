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
import com.here.xyz.hub.Service;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.jwt.JWTOptions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.io.IOUtils;

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
    3. For the use with VertX remove the header & footer and trailing new-line chars from both files e.g.:
      -----BEGIN PRIVATE KEY----- #<--- remove this
      ...
      -----END PRIVATE KEY----- #<--- remove this

  */

  private static void setup() throws IOException {
    JWTAuthOptions authConfig = new JWTAuthOptions()
        .setJWTOptions(jwtOptions)
        .addPubSecKey(new PubSecKeyOptions()
            .setAlgorithm("RS256")
            .setPublicKey(readResourceFile("/auth/jwt.pub"))
            .setSecretKey(readResourceFile("/auth/jwt.key")));

    authProvider = JWTAuth.create(Service.vertx, authConfig);
  }

  private static String readResourceFile(String resourceFilename) throws IOException {
    return IOUtils.toString(JwtGenerator.class.getResourceAsStream(resourceFilename)).trim();
  }

  public static String generateToken(JWTPayload payload) {
    return authProvider.generateToken(
        new JsonObject(Json.mapper.convertValue(payload, new TypeReference<Map<String, Object>>() {})), jwtOptions);
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
