package com.here.xyz.pub.handlers;

import com.here.xyz.models.hub.Subscription;
import com.here.xyz.pub.models.JdbcConnectionParams;
import com.here.xyz.pub.models.PubConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SubscriptionHandler implements Runnable{
    private static final Logger logger = LogManager.getLogger();

    private PubConfig pubCfg;
    private JdbcConnectionParams adminDBConnParams;
    private Subscription sub;

    public SubscriptionHandler(final PubConfig pubCfg, final JdbcConnectionParams adminDBConnParams, final Subscription sub) {
        this.pubCfg = pubCfg;
        this.adminDBConnParams = adminDBConnParams;
        this.sub = sub;
    }

    // Called once per "active" subscription to be processed
    @Override
    public void run() {
        // TODO : Debug
        logger.debug("SubscriptionHandler got called for subId : {}", sub.getId());
        // TODO : Acquire distributed lock against subscription Id
        // If unsuccessful, then return gracefully (likely another thread is still processing this subscription)

        // TODO : Fetch last txn_id from SpaceDB::xyz_config::xyz_txn_pub table
        // if not entry found, then start with -1

        // TODO : Fetch all new transactions (in right order) from SpaceDB::xyz_config::xyz_txn and xyz_txn_data tables
        // if no new transactions found, then return gracefully

        // TODO : Handover transactions to appropriate Publisher (e.g. DefaultSNSPublisher)

        // TODO : Update last txn_id in SpaceDB::xyz_config::xyz_txn_pub table

        // TODO : Release lock against subscription Id
    }

}
