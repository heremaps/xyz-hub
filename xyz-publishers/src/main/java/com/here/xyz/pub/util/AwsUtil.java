package com.here.xyz.pub.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import java.util.HashMap;
import java.util.Map;

public class AwsUtil {
    private static final Logger logger = LogManager.getLogger();

    // Cache of AWS Region <> SnsAsyncClient
    private static Map<String, SnsAsyncClient> snsClientMap = new HashMap<>();

    public static SnsAsyncClient getSnsAsyncClient(final String regionStr) {
        SnsAsyncClient snsClient = snsClientMap.get(regionStr);
        if (snsClient == null) {
            snsClient = createAndCacheSnsclient(regionStr);
        }
        return snsClient;
    }

    // "synchronized" block to prevent concurrent SnsClient creation for same region
    private synchronized static SnsAsyncClient createAndCacheSnsclient(final String regionStr) {
        SnsAsyncClient snsClient = snsClientMap.get(regionStr);
        if (snsClient == null) {
            // create new client and add it to cache
            final Region r = Region.of(regionStr);
            snsClient = SnsAsyncClient.builder().region(r).build();
            snsClientMap.put(regionStr, snsClient);
        }
        return snsClient;
    }

}
