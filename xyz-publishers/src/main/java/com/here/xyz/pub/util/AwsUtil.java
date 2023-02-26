package com.here.xyz.pub.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class AwsUtil {
    private static final Logger logger = LogManager.getLogger();

    // Cache of AWS Region <> SnsAsyncClient
    private static final Map<String, SnsAsyncClient> snsClientMap = new HashMap<>();
    private static final Map<String, Long> snsClientExpiryMap = new HashMap<>();

    public static SnsAsyncClient getSnsAsyncClient(final String regionStr) {
        SnsAsyncClient snsClient = snsClientMap.get(regionStr);
        if (snsClient == null || System.currentTimeMillis() > snsClientExpiryMap.get(regionStr)) {
            // SNS client not in cache OR the time has expired
            snsClient = createAndCacheSnsClient(regionStr);
        }
        return snsClient;
    }

    // "synchronized" block to prevent concurrent SnsClient creation for same region
    private synchronized static SnsAsyncClient createAndCacheSnsClient(final String regionStr) {
        SnsAsyncClient snsClient = snsClientMap.get(regionStr);
        if (snsClient == null || System.currentTimeMillis() > snsClientExpiryMap.get(regionStr)) {
            if (snsClient != null) {
                // close previous client if exists (as the time has expired)
                logger.info("Recreating AWS SNS client for {}", regionStr);
                snsClient.close();
                snsClient = null;
            }
            // create new client and add it to cache
            final Region r = Region.of(regionStr);
            snsClient = SnsAsyncClient.builder().region(r).build();
            final int expiryMins = 15;
            snsClientExpiryMap.put(regionStr, System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(expiryMins));
            snsClientMap.put(regionStr, snsClient);
            logger.info("AWS SNS client created for {} with expiry of {}mins", regionStr, expiryMins);
        }
        return snsClient;
    }

}
