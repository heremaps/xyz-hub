package com.here.xyz.pub.handlers;

import com.here.xyz.models.hub.Subscription;
import com.here.xyz.pub.jdbc.PubDatabaseHandler;
import com.here.xyz.pub.models.JdbcConnectionParams;
import com.here.xyz.pub.models.PubConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class PubJobHandler implements Runnable {
    private static final Logger logger = LogManager.getLogger();

    private PubConfig pubCfg;
    private JdbcConnectionParams adminDBConnParams;
    // Subscription handling Thread Pool
    private static ThreadPoolExecutor subHandlingPool;



    public PubJobHandler(final PubConfig pubCfg, final JdbcConnectionParams adminDBConnParams) {
        this.pubCfg = pubCfg;
        this.adminDBConnParams = adminDBConnParams;
    }

    // Called once per job execution, to fetch and process all "active" subscriptions
    @Override
    public void run() {
        logger.debug("Starting publisher job...");
        try {

            // Fetch all active subscriptions from AdminDB::xyz_config::xyz_subscription table
            List<Subscription> subList = PubDatabaseHandler.fetchAllSubscriptions(adminDBConnParams);
            if (subList == null || subList.isEmpty()) {
                logger.debug("No active subscriptions to be processed.");
                return;
            }

            // TODO : Debug
            logger.info("{} active subscriptions to be processed.", subList.size());

            // Distribute subscriptions amongst thread pool to perform parallel publish (configurable poolSize e.g. 10 threads)
            distributeSubscriptionProcessing(subList);

            // TODO : Debug
            logger.info("All subscriptions were submitted");
        }
        catch (Exception ex) {
            logger.error("Exception while running Publisher job. ", ex);
        }
        logger.debug("Publisher job finished!");
    }



    // Blocking function which distributes subscriptions to a thread pool and waits for all threads to complete
    private void distributeSubscriptionProcessing(final List<Subscription> subList) throws InterruptedException, ExecutionException {
        // create thread pool (if doesn't exist already)
        if (subHandlingPool == null) {
            subHandlingPool = new ThreadPoolExecutor(pubCfg.TXN_PUB_TPOOL_CORE_SIZE,
                    pubCfg.TXN_PUB_TPOOL_MAX_SIZE,
                    pubCfg.TXN_PUB_TPOOL_KEEP_ALIVE_SEC,
                    TimeUnit.SECONDS,
                    new SynchronousQueue<>(), // queue with zero capacity
                    new ThreadPoolExecutor.CallerRunsPolicy()); // on reaching queue limit, caller thread itself is used for execution
        }
        // distribute subscritions to thread pool
        List<Future> fList = new ArrayList<Future>(subList.size());
        for (final Subscription sub : subList) {
            // TODO : Remove
            logger.info("Subscription to be submitted to thread for subId : {}", sub.getId());
            final Future f = subHandlingPool.submit(new PubSubscriptionHandler(pubCfg, adminDBConnParams, sub));
            fList.add(f);
            // TODO : Remove
            logger.info("Subscription submitted to thread for subId : {}", sub.getId());
        }
        // Wait for thread completion
        for (Future f : fList) {
            f.get();
        }
    }

}
