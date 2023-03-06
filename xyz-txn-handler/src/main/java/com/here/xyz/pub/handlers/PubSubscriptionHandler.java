package com.here.xyz.pub.handlers;

import com.here.xyz.models.hub.Subscription;
import com.here.xyz.pub.db.PubDatabaseHandler;
import com.here.xyz.pub.db.PubJdbcConnectionPool;
import com.here.xyz.pub.models.JdbcConnectionParams;
import com.here.xyz.pub.models.PubConfig;
import com.here.xyz.pub.models.PubTransactionData;
import com.here.xyz.pub.models.PublishEntryDTO;
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
        final String spaceId = sub.getSource();
        boolean lockAcquired = false;
        Connection lockConn = null;
        Thread.currentThread().setName("pub-job-subId-"+subId);

        logger.debug("Starting publisher for subscription Id [{}], spaceId [{}]...", subId, spaceId);
        try {
            // Acquire distributed lock against subscription Id
            // If unsuccessful, then return gracefully (likely another thread is processing this subscription)
            lockConn = PubJdbcConnectionPool.getConnection(adminDBConnParams);
            lockAcquired = PubDatabaseHandler.advisoryLock(subId, lockConn);
            if (!lockAcquired) {
                logger.debug("Couldn't acquire lock for subscription Id [{}]. Some other thread might be processing the same.", subId);
                return;
            }

            // Fetch last txn_id from AdminDB::xyz_config::xyz_txn_pub table
            // if no entry found, then start with -1
            PublishEntryDTO lastTxn = PubDatabaseHandler.fetchLastTxnIdForSubId(subId, adminDBConnParams);
            logger.debug("For subscription Id [{}], spaceId [{}], the LastTxnId obtained as [{}]", subId, spaceId, lastTxn);

            // Fetch SpaceDB Connection details from AdminDB::xyz_config::xyz_space and xyz_storage tables
            // if no entry found, then log error and return
            JdbcConnectionParams spaceDBConnParams = PubDatabaseHandler.fetchDBConnParamsForSpaceId(spaceId, adminDBConnParams);
            logger.debug("Subscription Id [{}], spaceId [{}], to be processed against database {} with user {}",
                    subId, spaceId, spaceDBConnParams.getDbUrl(), spaceDBConnParams.getUser());

            // Fetch all new transactions (in right order) from SpaceDB::xyz_config::xyz_transactions and space tables
            // if no new transactions found, then return gracefully
            List<PubTransactionData> txnList = null;
            boolean txnFound = false;
            while (
                (txnList =
                    PubDatabaseHandler.fetchPublishableTransactions(spaceDBConnParams, spaceId, lastTxn)
                ) != null
            ) {
                logger.info("Fetched [{}] publishable records for subId [{}], space [{}]",
                        txnList.size(), subId, spaceId);
                txnFound = true;
                // Handover transactions to appropriate Publisher (e.g. DefaultSNSPublisher)
                lastTxn = PubUtil.getPubInstance(sub).publishTransactions(pubCfg, sub, txnList, lastTxn.getLastTxnId(), lastTxn.getLastTxnRecId());

                // Update last txn_id in AdminDB::xyz_config::xyz_txn_pub table
                PubDatabaseHandler.saveLastTxnId(adminDBConnParams, subId, lastTxn);
            }
            if (!txnFound) {
                logger.debug("No publishable transactions found for subId [{}], space [{}]", subId, spaceId);
                // No transaction found. Make an insert into publisher table with lastTxnId as -1
                PubDatabaseHandler.saveLastTxnId(adminDBConnParams, subId, lastTxn);
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
        logger.debug("Publisher job completed for subscription Id [{}], spaceId [{}]...", subId, spaceId);
        return;
    }




}
