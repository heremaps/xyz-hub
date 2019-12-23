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

package com.here.xyz.connectors;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * When you need to run you AWS lambda locally you may use this class an pass it to the AWS lambda handler as context.
 */
@SuppressWarnings("unused")
public class SimulatedContext implements Context, LambdaLogger {

  private static final Logger logger = LoggerFactory.getLogger(AbstractConnectorHandler.class);
  private final Map<String, String> environmentVariables;
  private String getLogStreamName;
  private String logGroupName;
  private String awsRequestId;
  private String functionName;
  private String functionVersion;
  private String invokedFunctionArn;

  public SimulatedContext(String functionName, Map<String, String> environmentVariables) {
    this.environmentVariables = environmentVariables;
    this.functionName = functionName;
  }

  /**
   * Returns the mocked environment variable.
   */
  public String getEnv(String name) {
    return environmentVariables.get(name);
  }

  @Override
  public String getAwsRequestId() {
    if (awsRequestId == null) {
      awsRequestId = RandomStringUtils.random(10);
    }
    return awsRequestId;
  }

  @Override
  public String getLogGroupName() {
    if (logGroupName == null) {
      logGroupName = "embedded";
    }
    return logGroupName;
  }

  @Override
  public String getLogStreamName() {
    if (getLogStreamName == null) {
      getLogStreamName = RandomStringUtils.random(10);
    }
    return getLogStreamName;
  }

  @Override
  public String getFunctionName() {
    return functionName;
  }

  @Override
  public String getFunctionVersion() {
    if (functionVersion == null) {
      functionVersion = "1.0";
    }
    return functionVersion;
  }

  @Override
  public String getInvokedFunctionArn() {
    if (invokedFunctionArn == null) {
      invokedFunctionArn = "arn:embedded";
    }
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
    } catch (CharacterCodingException ignored) {
    }
  }
}
