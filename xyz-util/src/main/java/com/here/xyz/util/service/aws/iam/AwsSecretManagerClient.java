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

import io.vertx.core.json.JsonObject;
import java.net.URI;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClientBuilder;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

public class AwsSecretManagerClient {
    private final String region;
    private final String endpointOverride;
    private final SecretsManagerClient client;

    public AwsSecretManagerClient(String region) {
        this(region, null);
    }

    public AwsSecretManagerClient(String region, String endpointOverride) {
        this.region = region;
        this.endpointOverride = endpointOverride;

        SdkHttpClient httpClient = ApacheHttpClient.builder().build();

        SecretsManagerClientBuilder builder = SecretsManagerClient.builder()
                .region(Region.of(region))
                .httpClient(httpClient);

        if (endpointOverride != null) {
            builder.endpointOverride(URI.create(endpointOverride))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("localstack", "localstack")));
        }

        this.client = builder.build();
    }

    private String getSecret(String secretArn) {
        GetSecretValueRequest request = GetSecretValueRequest.builder()
                .secretId(secretArn)
                .build();
        GetSecretValueResponse result = client.getSecretValue(request);
        return result.secretString();
    }

    /**
     *
     * The secret should contain json string in the format below <br>
     *
     * {"accessKey": "aws_access_key_id_value", "secretKey": "aws_secret_access_key_value"}
     *
     * @param secretArn the ARN of the AWS secret
     * @return the AWSCredenitials created from the secret
     *
     */
    public AwsCredentials getCredentialsFromSecret(String secretArn) {
        JsonObject secretJson = new JsonObject(getSecret(secretArn));
        return AwsBasicCredentials.create(secretJson.getString("accessKey"), secretJson.getString("secretKey"));
    }
}
