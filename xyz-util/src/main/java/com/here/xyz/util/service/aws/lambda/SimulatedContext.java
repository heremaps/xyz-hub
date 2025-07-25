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

package com.here.xyz.util.service.aws.lambda;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * When you need to run your connector locally you may use this class and pass an instance of it to the connector handler as context.
 */
@SuppressWarnings("unused")
public class SimulatedContext implements Context, LambdaLogger {
  private static final Logger logger = LogManager.getLogger();
  private final Map<String, String> environmentVariableOverrides;
  private String getLogStreamName;
  private String logGroupName;
  private String awsRequestId;
  private String functionName;
  private String functionVersion;
  private String invokedFunctionArn;

  public SimulatedContext(String functionName, Map<String, String> environmentVariableOverrides) {
    this.environmentVariableOverrides = environmentVariableOverrides;
    this.functionName = functionName;
  }

  /**
   * Returns environment variable. (mocked if applicable)
   */
  public final String getEnv(String variableName) {
    if (environmentVariableOverrides != null && environmentVariableOverrides.containsKey(variableName))
      return environmentVariableOverrides.get(variableName);
    return System.getenv(variableName);
  }

  @Override
  public String getAwsRequestId() {
    if (awsRequestId == null)
      awsRequestId = RandomStringUtils.random(10);
    return awsRequestId;
  }

  @Override
  public String getLogGroupName() {
    if (logGroupName == null)
      logGroupName = "embedded";
    return logGroupName;
  }

  @Override
  public String getLogStreamName() {
    if (getLogStreamName == null)
      getLogStreamName = RandomStringUtils.random(10);
    return getLogStreamName;
  }

  @Override
  public String getFunctionName() {
    return functionName;
  }

  @Override
  public String getFunctionVersion() {
    if (functionVersion == null)
      functionVersion = "0";
    return functionVersion;
  }

  @Override
  public String getInvokedFunctionArn() {
    if (invokedFunctionArn == null)
      invokedFunctionArn = "arn:embedded";
    return invokedFunctionArn;
  }

  @Override
  public CognitoIdentity getIdentity() {
    return null;
  }

  @Override
  public ClientContext getClientContext() {
    return null;
  }

  @Override
  public int getRemainingTimeInMillis() {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMemoryLimitInMB() {
    return (int) Runtime.getRuntime().maxMemory() / (1024 * 1024);
  }

  @Override
  public LambdaLogger getLogger() {
    return this;
  }

  @Override
  public void log(String message) {
    logger.info(message);
  }

  @Override
  public void log(byte[] bytes) {
    try {
      final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
      decoder.onMalformedInput(CodingErrorAction.REPORT);
      decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
      final ByteBuffer in = ByteBuffer.wrap(bytes);
      log(decoder.decode(in).toString());
    }
    catch (CharacterCodingException ignored) {
      //Ignore
    }
  }
}
