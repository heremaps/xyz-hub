package com.here.xyz.pub;

import com.here.xyz.pub.handlers.PubJobHandler;
import com.here.xyz.pub.handlers.SeqJobHandler;
import com.here.xyz.pub.models.JdbcConnectionParams;
import com.here.xyz.pub.models.PubConfig;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/*
* Class responsible for periodically running couple of background jobs.
* 1) Sequencer Job:
*       It adds incrementing sequence number to newer entries entering in transactions table.
*       It spawns multiple threads, subject to thread pool capacity, with one thread per connector (i.e. per SpaceDB).
* 2) Publisher Job:
*       It publishes transactions onto respective destinations subscribed as per xyz_subscriptions table.
*       It spawns multiple threads, subject to thread pool capacity, with one thread per subscription.
*/
public class XYZTransactionHandler {
    private static final Logger logger = LogManager.getLogger();
    // holds singleton instance
    private static XYZTransactionHandler handler;

    private static JsonObject rawConfig;

    private static PubConfig pubCfg;

    private static JdbcConnectionParams adminDBConnParams;



    // empty constructor to prevent external instantiation of singleton
    private XYZTransactionHandler(final JsonObject config) {
        rawConfig = config;
        readConfig();
    }


    // create and return singleton instance
    public static synchronized XYZTransactionHandler getInstance(final JsonObject config) {
        if (handler == null) {
            handler = new XYZTransactionHandler(config);
        }
        return handler;
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
        // Set AWS account access details
        System.setProperty("aws.accessKeyId", pubCfg.AWS_ACCESS_KEY_ID);
        System.setProperty("aws.secretAccessKey", pubCfg.AWS_SECRET_ACCESS_KEY);
    }


    // Starts the periodic publisher job (if enabled in config)
    public void start() {
        // Start sequencer job (if enabled)
        if (!pubCfg.ENABLE_TXN_SEQUENCER) {
            logger.warn("As per config, Transaction Sequencer is not enabled.");
            return;
        }
        else {
            // Schedule Sequencer job (as per configured frequency e.g. 2 secs)
            new ScheduledThreadPoolExecutor(1)
                    .scheduleWithFixedDelay(
                            new SeqJobHandler(pubCfg, adminDBConnParams),
                            pubCfg.TXN_SEQ_JOB_INITIAL_DELAY_MS, pubCfg.TXN_SEQ_JOB_SUBSEQUENT_DELAY_MS, TimeUnit.MILLISECONDS
                    );
            logger.info("Transaction Sequencer job is set to start after {}ms with subsequent delay of {}ms.",
                    pubCfg.TXN_SEQ_JOB_INITIAL_DELAY_MS, pubCfg.TXN_SEQ_JOB_SUBSEQUENT_DELAY_MS);
        }

        // Start publisher job (if enabled)
        if (!pubCfg.ENABLE_TXN_PUBLISHER) {
            logger.warn("As per config, Transaction Publisher is not enabled.");
            return;
        }
        else {
            // Schedule Publisher job (as per configured frequency e.g. 2 secs)
            new ScheduledThreadPoolExecutor(1)
                    .scheduleWithFixedDelay(
                            new PubJobHandler(pubCfg, adminDBConnParams),
                            pubCfg.TXN_PUB_JOB_INITIAL_DELAY_MS, pubCfg.TXN_PUB_JOB_SUBSEQUENT_DELAY_MS, TimeUnit.MILLISECONDS
                    );
            logger.info("Transaction Publisher job is set to start after {}ms with subsequent delay of {}ms.",
                    pubCfg.TXN_PUB_JOB_INITIAL_DELAY_MS, pubCfg.TXN_PUB_JOB_SUBSEQUENT_DELAY_MS);
        }
    }

}
