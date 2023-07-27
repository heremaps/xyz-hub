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
import com.here.xyz.hub.util.ARN;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AwsSecretManagerClient {
    private static final Logger logger = LogManager.getLogger();

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

    public String getSecret(ARN secretArn) {
        String resource = secretArn.getResourceWithoutType();
        String secretName = resource.substring(0, resource.lastIndexOf('-'));
        return getSecret(secretName);
    }

    public String getSecret(String secretName) {
        GetSecretValueResult result = client.getSecretValue(new GetSecretValueRequest().withSecretId(secretName));
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
    public AWSCredentials getCredentialsFromSecret(ARN secretArn) {
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
