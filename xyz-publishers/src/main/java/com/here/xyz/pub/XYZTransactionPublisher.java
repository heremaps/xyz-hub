package com.here.xyz.pub;

import com.here.xyz.pub.handlers.PubJobHandler;
import com.here.xyz.pub.models.JdbcConnectionParams;
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

    private static JdbcConnectionParams adminDBConnParams;



    // empty constructor to prevent external instantiation of singleton
    private XYZTransactionPublisher(final JsonObject config) {
        rawConfig = config;
        readConfig();
    }


    // create and return singleton instance
    public static synchronized XYZTransactionPublisher getInstance(final JsonObject config) {
        if (pub == null) {
            pub = new XYZTransactionPublisher(config);
        }
        return pub;
    }


    // Read and validate publisher specific config
    private static void readConfig() {
        pubCfg = rawConfig.mapTo(PubConfig.class);
        // Read AdminDB connection params
        adminDBConnParams = new JdbcConnectionParams();
        adminDBConnParams.setSpaceId("XYZ_ADMIN_DB");
        adminDBConnParams.setDbUrl(pubCfg.STORAGE_DB_URL);
        adminDBConnParams.setUser(pubCfg.STORAGE_DB_USER);
        adminDBConnParams.setPswd(pubCfg.STORAGE_DB_PASSWORD);
    }


    // Starts the periodic publisher job (if enabled in config)
    public void start() {
        if (!pubCfg.ENABLE_TXN_PUBLISHER) {
            logger.warn("As per config, Transaction Publisher is not enabled.");
            return;
        }
        // Schedule Publisher job (as per configured frequency e.g. 2 secs)
        new ScheduledThreadPoolExecutor(1)
                .scheduleWithFixedDelay(
                        new PubJobHandler(pubCfg, adminDBConnParams),
                        0, pubCfg.TXN_PUB_JOB_FREQ_MS, TimeUnit.MILLISECONDS
                );
    }

}
