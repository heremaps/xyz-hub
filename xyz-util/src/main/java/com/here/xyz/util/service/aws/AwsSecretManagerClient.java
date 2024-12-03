package com.here.xyz.util.service.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import io.vertx.core.json.JsonObject;

@Deprecated
public class AwsSecretManagerClient {
    private final String region;
    private final String endpointOverride;
    private final AWSSecretsManager client;

    public AwsSecretManagerClient(String region) {
        this(region, null);
    }

    public AwsSecretManagerClient(String region, String endpointOverride) {
        this.region = region;
        this.endpointOverride = endpointOverride;

        AWSSecretsManagerClientBuilder builder = AWSSecretsManagerClientBuilder.standard();

        if (endpointOverride != null)
            builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpointOverride, region))
                    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("localstack", "localstack")));

        this.client = builder.build();
    }

    private String getSecret(String secretArn) {
        GetSecretValueResult result = client.getSecretValue(new GetSecretValueRequest().withSecretId(secretArn));
        return result.getSecretString();
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
    public AWSCredentials getCredentialsFromSecret(String secretArn) {
        JsonObject secretJson = new JsonObject(getSecret(secretArn));
        return new BasicAWSCredentials(secretJson.getString("accessKey"), secretJson.getString("secretKey"));
    }
}
