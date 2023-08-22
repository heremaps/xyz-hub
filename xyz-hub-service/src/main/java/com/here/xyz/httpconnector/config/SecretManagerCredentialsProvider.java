package com.here.xyz.httpconnector.config;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.here.xyz.httpconnector.CService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class SecretManagerCredentialsProvider implements AWSCredentialsProvider  {

    private static final Logger logger = LogManager.getLogger();
    private static final int DEFAULT_REFRESH_INTERVAL_SECONDS = 3600;

    private final AtomicReference<AWSCredentials> credentialsRef;
    private final ScheduledExecutorService scheduler;

    private String secretArn;

    public SecretManagerCredentialsProvider(String secretArn) {
        this(secretArn, DEFAULT_REFRESH_INTERVAL_SECONDS);
    }

    public SecretManagerCredentialsProvider(String secretArn, long refreshInterval) {

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
            AWSCredentials newCredentials = CService.jobSecretClient.getCredentialsFromSecret(secretArn);
            credentialsRef.set(newCredentials);
        } catch (Exception e) {
            logger.error("Failed to refresh credentials from secret manager! ", e);
        }
    }
}
