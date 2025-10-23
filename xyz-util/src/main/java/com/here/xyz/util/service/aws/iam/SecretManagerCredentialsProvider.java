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

package com.here.xyz.util.service.aws.iam;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class SecretManagerCredentialsProvider implements AwsCredentialsProvider {
    private static final Logger logger = LogManager.getLogger();
    private static final int DEFAULT_REFRESH_INTERVAL_SECONDS = 3600;

    private final AtomicReference<AwsCredentials> credentialsRef;
    private final ScheduledExecutorService scheduler;

    private String secretArn;

    /**
     * The client to access secrets from AWS Secret Manager
     */
    private static AwsSecretManagerClient jobSecretClient;

    public SecretManagerCredentialsProvider(String region, String endpointOverride, String secretArn) {
        this(region, endpointOverride, secretArn, DEFAULT_REFRESH_INTERVAL_SECONDS);
    }

    public SecretManagerCredentialsProvider(String region, String endpointOverride, String secretArn, long refreshInterval) {
        if (jobSecretClient == null)
            jobSecretClient = new AwsSecretManagerClient(region, endpointOverride);

        this.secretArn = secretArn;
        this.credentialsRef = new AtomicReference<>();
        this.scheduler = Executors.newScheduledThreadPool(1);

        scheduleCredentialsRefresh(refreshInterval);
    }

    private void scheduleCredentialsRefresh(long refreshInterval) {
        scheduler.scheduleAtFixedRate(this::refresh, 0, refreshInterval, TimeUnit.SECONDS);
    }

    @Override
    public AwsCredentials resolveCredentials() {
        AwsCredentials currentCredentials = credentialsRef.get();

        if(currentCredentials == null) {
            refresh();
            currentCredentials = credentialsRef.get();
        }

        return currentCredentials;
    }

    public void refresh() {
        try {
            AwsCredentials newCredentials = jobSecretClient.getCredentialsFromSecret(secretArn);
            credentialsRef.set(newCredentials);
        } catch (Exception e) {
            logger.error("Failed to refresh credentials from secret manager! ", e);
        }
    }
}
