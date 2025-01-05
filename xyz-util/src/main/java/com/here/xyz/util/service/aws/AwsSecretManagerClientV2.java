package com.here.xyz.util.service.aws;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClientBuilder;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import io.vertx.core.json.JsonObject;

import java.net.URI;

public class AwsSecretManagerClientV2 {
    private final SecretsManagerClient client;

    public AwsSecretManagerClientV2(String region) {
        this(region, null);
    }

    public AwsSecretManagerClientV2(String region, String endpointOverride) {

        SecretsManagerClientBuilder builder = SecretsManagerClient.builder()
                .region(Region.of(region));

        if (endpointOverride != null) {
            builder.endpointOverride(URI.create(endpointOverride))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("localstack", "localstack")));
        }

        this.client = builder.build();
    }

    private String getSecret(String secretArn) {
        GetSecretValueResponse response = client.getSecretValue(
                GetSecretValueRequest.builder().secretId(secretArn).build());
        return response.secretString();
    }

    /**
     * The secret should contain json string in the format below <br>
     * <p>
     * {"accessKey": "aws_access_key_id_value", "secretKey": "aws_secret_access_key_value"}
     *
     * @param secretArn the ARN of the AWS secret
     * @return the AWSCredenitials created from the secret
     */
    public AwsBasicCredentials getCredentialsFromSecret(String secretArn) {
        JsonObject secretJson = new JsonObject(getSecret(secretArn));
        return AwsBasicCredentials.create(secretJson.getString("accessKey"), secretJson.getString("secretKey"));
    }
}
