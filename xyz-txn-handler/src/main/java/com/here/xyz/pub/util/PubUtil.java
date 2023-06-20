package com.here.xyz.pub.util;

import com.here.naksha.lib.core.models.hub.Subscription;
import com.here.xyz.pub.impl.DefaultSNSPublisher;
import com.here.xyz.pub.impl.IPublisher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class PubUtil {
    private static final Logger logger = LogManager.getLogger();

    private static Map<String, IPublisher> pubMap = new HashMap<>();


    public static IPublisher getPubInstance(final Subscription sub) {
        Map<String, Object> paramsMap = null;
        String pubType = null;
        // find out, which publisher impl we have to use
        if (sub == null
                || sub.getConfig() == null
                || (paramsMap = sub.getConfig().getParams()) == null
                || (pubType = paramsMap.get("pubType").toString()) == null
        ) {
            throw new RuntimeException("pubType not configured for subId "+sub.getId());
        }

        // reuse impl class from cache (if available)
        IPublisher publisher = pubMap.get(pubType);
        if (publisher == null) {
            // create new impl instance and add it in cache
            switch (pubType) {
                case "DEFAULT-SNS-PUBLISHER":
                    publisher = new DefaultSNSPublisher();
                    break;
                default:
                    throw new RuntimeException("Unsupported pubType ["+pubType+"] for subscription id "+sub.getId());
            }
            pubMap.put(pubType, publisher);
        }
        return publisher;
    }


    public static String getSnsTopicARN(final Subscription sub) {
        Map<String, Object> paramsMap = null;
        String topicArn = null;
        // find out, which publisher impl we have to use
        if (sub == null
                || sub.getConfig() == null
                || (paramsMap = sub.getConfig().getParams()) == null
                || (topicArn = paramsMap.get("destination").toString()) == null
                || topicArn.equals("")
        ) {
            throw new RuntimeException("destination not configured for subId "+sub.getId());
        }

        return topicArn;
    }

}
