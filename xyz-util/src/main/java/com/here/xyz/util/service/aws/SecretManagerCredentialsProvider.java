package com.here.xyz.util.service.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SecretManagerCredentialsProvider implements AWSCredentialsProvider  {
    private static final Logger logger = LogManager.getLogger();
    private static final int DEFAULT_REFRESH_INTERVAL_SECONDS = 3600;

    private final AtomicReference<AWSCredentials> credentialsRef;
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
    public AWSCredentials getCredentials() {

        AWSCredentials currentCredentials = credentialsRef.get();

        if(currentCredentials == null) {
            refresh();
            currentCredentials = credentialsRef.get();
        }

        return currentCredentials;

    }

    @Override
    public void refresh() {
        try {
            AWSCredentials newCredentials = jobSecretClient.getCredentialsFromSecret(secretArn);
            credentialsRef.set(newCredentials);
        } catch (Exception e) {
            logger.error("Failed to refresh credentials from secret manager! ", e);
        }
    }
}
