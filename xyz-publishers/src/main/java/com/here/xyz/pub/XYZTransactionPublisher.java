package com.here.xyz.pub;

import com.here.xyz.pub.handlers.PublisherJob;
import com.here.xyz.pub.models.PubConfig;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class XYZTransactionPublisher {
    private static final Logger logger = LogManager.getLogger();
    // holds singleton instance
    private static XYZTransactionPublisher pub;

    private static JsonObject rawConfig;

    private static PubConfig pubCfg;



    // empty constructor to prevent external instantiation of singleton
    private XYZTransactionPublisher(final JsonObject config) {
        rawConfig = config;
    }


    // create and return singleton instance
    public static synchronized XYZTransactionPublisher getInstance(final JsonObject config) {
        if (pub == null) {
            pub = new XYZTransactionPublisher(config);
            pub.readConfig();
        }
        return pub;
    }


    // Read and validate publisher specific config
    public static void readConfig() {
        pubCfg = rawConfig.mapTo(PubConfig.class);
    }


    // Starts the periodic publisher job (if enabled in config)
    public void start() {
        if (!pubCfg.ENABLE_TXN_PUBLISHER) {
            logger.warn("Transaction Publisher is not enabled.");
            return;
        }
        // TODO : Schedule job to execute processAllSubscriptions() function (configurable freq. e.g. 1 sec)
        new ScheduledThreadPoolExecutor(1)
                .scheduleWithFixedDelay(new PublisherJob(), 0, pubCfg.TXN_PUB_JOB_FREQ_MS, TimeUnit.MILLISECONDS);
    }

}
