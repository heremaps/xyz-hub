package com.here.xyz.pub.handlers;

import com.here.xyz.models.hub.Subscription;
import com.here.xyz.pub.db.PubDatabaseHandler;
import com.here.xyz.pub.db.PubJdbcConnectionPool;
import com.here.xyz.pub.models.JdbcConnectionParams;
import com.here.xyz.pub.models.PubConfig;
import com.here.xyz.pub.models.PubTransactionData;
import com.here.xyz.pub.util.PubUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class PubSubscriptionHandler implements Runnable{
    private static final Logger logger = LogManager.getLogger();

    private PubConfig pubCfg;
    private JdbcConnectionParams adminDBConnParams;
    private Subscription sub;

    public PubSubscriptionHandler(final PubConfig pubCfg, final JdbcConnectionParams adminDBConnParams, final Subscription sub) {
        this.pubCfg = pubCfg;
        this.adminDBConnParams = adminDBConnParams;
        this.sub = sub;
    }

    // Called once per "active" subscription to be processed
    @Override
    public void run() {
        final String subId = sub.getId();
        boolean lockAcquired = false;
        Connection lockConn = null;

        // TODO : Debug
        logger.info("Starting publisher for subscription Id [{}]...", subId);
        try {
            // Acquire distributed lock against subscription Id
            // If unsuccessful, then return gracefully (likely another thread is processing this subscription)
            lockConn = PubJdbcConnectionPool.getConnection(adminDBConnParams);
            lockAcquired = PubDatabaseHandler.advisoryLock(subId, lockConn);
            if (!lockAcquired) {
                // TODO : Debug
                logger.warn("Couldn't acquire lock for subscription Id [{}]. Some other thread might be processing the same.", subId);
                return;
            }

            // TODO : Fetch last txn_id from AdminDB::xyz_config::xyz_txn_pub table
            // if no entry found, then start with -1
            long lastTxnId = PubDatabaseHandler.fetchLastTxnIdForSubId(subId, adminDBConnParams);
            // TODO : Debug
            logger.info("For subscription Id [{}] the LastTxnId obtained as [{}]", subId, lastTxnId);

            // Fetch SpaceDB Connection details from AdminDB::xyz_config::xyz_space and xyz_storage tables
            // if no entry found, then log error and return
            JdbcConnectionParams spaceDBConnParams = PubDatabaseHandler.fetchDBConnParamsForSpaceId(sub.getSource(), adminDBConnParams);
            // TODO : Debug
            logger.info("Subscription Id [{}] to be processed against database {} with user {}",
                    subId, spaceDBConnParams.getDbUrl(), spaceDBConnParams.getUser());

            // TODO : Wait for ongoing transactions (lower txn_id's which are not yet committed),
            // instead of directly moving to next txn_id (which can result into losing out on messages)

            // Fetch all new transactions (in right order) from SpaceDB::xyz_config::xyz_txn and xyz_txn_data tables
            // if no new transactions found, then return gracefully
            List<PubTransactionData> txnList = null;
            boolean txnFound = false;
            while (
                (txnList =
                    PubDatabaseHandler.fetchPublishableTransactions(spaceDBConnParams, sub.getSource(), lastTxnId)
                ) != null
            ) {
                logger.info("Fetched [{}] publishable records for subId [{}], space [{}]",
                        txnList.size(), sub.getId(), sub.getSource());
                txnFound = true;
                // Handover transactions to appropriate Publisher (e.g. DefaultSNSPublisher)
                lastTxnId = PubUtil.getPubInstance(sub).publishTransactions(sub, txnList, lastTxnId);

                // Update last txn_id in AdminDB::xyz_config::xyz_txn_pub table
                PubDatabaseHandler.saveLastTxnId(adminDBConnParams, sub.getId(), lastTxnId);
            }
            if (!txnFound) {
                // TODO : Debug
                logger.info("No publishable transactions found for subId [{}], space [{}]", sub.getId(), sub.getSource());
                // No transaction found. Make an insert into publisher table with lastTxnId as -1
                PubDatabaseHandler.saveLastTxnId(adminDBConnParams, sub.getId(), lastTxnId);
            }

        }
        catch (Exception ex) {
            logger.error("Exception in publisher job for subscription Id [{}]. ", subId, ex);
        }
        finally {
            // Release lock against subscription Id (if it was acquired)
            try {
                if (lockAcquired && !PubDatabaseHandler.advisoryUnlock(subId, lockConn)) {
                    logger.warn("Couldn't release lock for subscription Id [{}]. If problem persist, it might need manual intervention.", subId);
                }
                if (lockConn != null) {
                    lockConn.close();
                }
            } catch (SQLException e) {
                logger.warn("Exception while releasing publisher lock for subscription Id [{}]. If problem persist, it might need manual intervention.", subId);
            }
        }
        // TODO : Debug
        logger.info("Publisher job completed for subscription Id [{}]...", subId);
        return;
    }




}
