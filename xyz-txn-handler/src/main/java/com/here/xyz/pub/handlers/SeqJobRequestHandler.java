package com.here.xyz.pub.handlers;

import com.here.xyz.pub.db.PubDatabaseHandler;
import com.here.xyz.pub.db.PubJdbcConnectionPool;
import com.here.xyz.pub.models.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.SQLException;

public class SeqJobRequestHandler implements Runnable{
    private static final Logger logger = LogManager.getLogger();

    private JdbcConnectionParams adminDBConnParams;
    private SeqJobRequest seqJobRequest;

    public SeqJobRequestHandler(final JdbcConnectionParams adminDBConnParams, final SeqJobRequest seqJobRequest) {
        this.adminDBConnParams = adminDBConnParams;
        this.seqJobRequest = seqJobRequest;
    }

    // Called once per SeqJobRequest to be processed
    @Override
    public void run() {
        final String dbUrl = seqJobRequest.getDbUrl();
        boolean lockAcquired = false;
        final String distinguisher = "XYZ_SEQUENCER"; // to distinguish resource in cache OR concurrency in DB locks
        Connection lockConn = null;
        Thread.currentThread().setName("seq-job-db-"+dbUrl);

        // TODO : Debug
        logger.info("Starting sequencer for DB [{}]...", dbUrl);
        try {
            // Prepare connDBParams for SpaceDB
            final JdbcConnectionParams spaceDBConnParams = new JdbcConnectionParams();
            spaceDBConnParams.setSpaceId(distinguisher+":"+dbUrl);
            spaceDBConnParams.setDbUrl(seqJobRequest.getDbUrl());
            spaceDBConnParams.setUser(seqJobRequest.getUser());
            spaceDBConnParams.setPswd(seqJobRequest.getPswd());

            // Acquire distributed lock for this sequencer job in SpaceDB (to ensure one job per DB instance)
            // If unsuccessful, then return gracefully (likely another thread is running sequencer)
            lockConn = PubJdbcConnectionPool.getConnection(spaceDBConnParams);
            lockAcquired = PubDatabaseHandler.advisoryLock(distinguisher, lockConn);
            if (!lockAcquired) {
                // TODO : Debug
                logger.warn("Couldn't acquire sequencer lock for DB [{}]. Some other thread might be processing the same.", dbUrl);
                return;
            }

            // Update id for newer entries in transactions table in a sequential order,
            // so a separate publisher thread can publish those newer entries/transactions
            // If table not found, then return gracefully (likely table is not created yet OR transactions not enabled yet in SpaceDB)
            PubDatabaseHandler.updateTransactionSequence(spaceDBConnParams, seqJobRequest);
        }
        catch (PSQLException pse) {
            if ("42P01".equals(pse.getSQLState())) {
                logger.debug("Exception in sequencer job for DB [{}]. ", dbUrl, pse);
            }
            else {
                logger.error("Exception in sequencer job for DB [{}]. ", dbUrl, pse);
            }
        }
        catch (Exception ex) {
            logger.error("Exception in sequencer job for DB [{}]. ", dbUrl, ex);
        }
        finally {
            // Release lock against subscription Id (if it was acquired)
            try {
                if (lockAcquired && !PubDatabaseHandler.advisoryUnlock(distinguisher, lockConn)) {
                    logger.warn("Couldn't release sequencer lock for DB [{}]. If problem persist, it might need manual intervention.", dbUrl);
                }
                if (lockConn != null) {
                    lockConn.close();
                }
            } catch (SQLException e) {
                logger.warn("Exception while releasing sequencer lock for DB [{}]. If problem persist, it might need manual intervention.", dbUrl);
            }
        }
        // TODO : Debug
        logger.info("Sequencer job completed for DB [{}]...", dbUrl);
        return;
    }




}
