package com.here.xyz.httpconnector.config;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.here.xyz.httpconnector.CService;
import io.vertx.core.json.JsonObject;

public class AwsSecretManagerClient {

    private final AWSSecretsManager client;

    public AwsSecretManagerClient() {
        AWSSecretsManagerClientBuilder builder = AWSSecretsManagerClientBuilder.standard();

        if(isLocal()){
            builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                            CService.configuration.LOCALSTACK_ENDPOINT, CService.configuration.JOBS_REGION))
                    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("localstack", "localstack")));
        }

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

    private boolean isLocal() {
        if(CService.configuration.HUB_ENDPOINT.contains("localhost") ||
                CService.configuration.HUB_ENDPOINT.contains("xyz-hub:8080"))
            return true;
        return false;
    }
}
