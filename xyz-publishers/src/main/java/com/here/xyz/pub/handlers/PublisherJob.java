package com.here.xyz.pub.handlers;

import com.here.xyz.models.hub.Subscription;
import com.here.xyz.pub.jdbc.PubJdbcConnectionPool;
import com.here.xyz.pub.models.JdbcConnectionParams;
import com.here.xyz.pub.models.PubConfig;
import io.vertx.core.json.Json;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class PublisherJob implements Runnable {
    private static final Logger logger = LogManager.getLogger();

    private PubConfig pubCfg;
    private JdbcConnectionParams adminDBConnParams;
    // Subscription handling Thread Pool
    private static ThreadPoolExecutor subHandlingPool;

    final private static String FETCH_ALL_SUBSCRIPTIONS =
            "SELECT s.id, s.source, s.config " +
            "FROM xyz_config.xyz_subscription s " +
            "WHERE s.status ->> 'state' = 'ACTIVE'";


    public PublisherJob(final PubConfig pubCfg, final JdbcConnectionParams adminDBConnParams) {
        this.pubCfg = pubCfg;
        this.adminDBConnParams = adminDBConnParams;
    }

    // Called once per job execution, to fetch and process all "active" subscriptions
    @Override
    public void run() {
        logger.debug("Starting publisher job...");

        try (final Connection conn = PubJdbcConnectionPool.getConnection(adminDBConnParams)) {

            // Fetch all active subscriptions from AdminDB::xyz_config::xyz_subscription table
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(FETCH_ALL_SUBSCRIPTIONS);
            List<Subscription> subList = new ArrayList<>();
            while (rs.next()) {
                final Subscription sub = new Subscription();
                sub.setId(rs.getString("id"));
                sub.setSource(rs.getString("source"));
                final String cfgJsonStr = rs.getString("config");
                sub.setConfig(Json.decodeValue(cfgJsonStr, Subscription.SubscriptionConfig.class));
                subList.add(sub);
            }
            rs.close();
            stmt.close();

            if (subList.isEmpty()) {
                logger.debug("No active subscriptions to be processed.");
                return;
            }

            // TODO : Debug
            logger.info("{} active subscriptions to be processed.", subList.size());

            // Distribute subscriptions amongst thread pool to perform parallel publish (configurable poolSize e.g. 10 threads)
            if (subHandlingPool == null) {
                subHandlingPool = new ThreadPoolExecutor(pubCfg.TXN_PUB_TPOOL_CORE_SIZE,
                        pubCfg.TXN_PUB_TPOOL_MAX_SIZE,
                        pubCfg.TXN_PUB_TPOOL_KEEP_ALIVE_SEC,
                        TimeUnit.SECONDS,
                        new SynchronousQueue<>(),
                        new ThreadPoolExecutor.CallerRunsPolicy());
            }
            List<Future> fList = new ArrayList<Future>(subList.size());
            for (final Subscription sub : subList) {
                // TODO : Remove
                logger.info("Subscription to be submitted to thread for subId : {}", sub.getId());
                final Future f = subHandlingPool.submit(new SubscriptionHandler(pubCfg, adminDBConnParams, sub));
                fList.add(f);
                // TODO : Remove
                logger.info("Subscription submitted to thread for subId : {}", sub.getId());
            }
            // Check for completion of all threads
            for (Future f : fList) {
                f.get();
            }
            // TODO : Debug
            logger.info("All subscriptions were submitted");
        }
        catch (Exception ex) {
            logger.error("Exception while running Publisher job. ", ex);
        }

        logger.debug("Publisher job finished!");
    }

}
