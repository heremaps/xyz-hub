package com.here.xyz.pub.handlers;

import com.here.xyz.pub.db.PubDatabaseHandler;
import com.here.xyz.pub.models.ConnectorDTO;
import com.here.xyz.pub.models.JdbcConnectionParams;
import com.here.xyz.pub.models.PubConfig;
import com.here.xyz.pub.models.SeqJobRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;

public class SeqJobHandler implements Runnable {
    private static final Logger logger = LogManager.getLogger();

    private PubConfig pubCfg;
    private JdbcConnectionParams adminDBConnParams;
    // Connector handling Thread Pool
    private static ThreadPoolExecutor connHandlingPool;



    public SeqJobHandler(final PubConfig pubCfg, final JdbcConnectionParams adminDBConnParams) {
        this.pubCfg = pubCfg;
        this.adminDBConnParams = adminDBConnParams;
    }

    // Called once per job execution, to fetch and process all "active" subscriptions
    @Override
    public void run() {
        Thread.currentThread().setName("seq-job");
        logger.debug("Starting sequencer job...");
        try {
            // Fetch Connector details for all SpaceDBs (i.e. excluding XYZ's own AdminDB)
            List<ConnectorDTO> connectorList = PubDatabaseHandler.fetchConnectorsAndSpaces(adminDBConnParams, pubCfg.DEFAULT_STORAGE_ID);
            if (connectorList == null || connectorList.isEmpty()) {
                logger.debug("No connectors to be processed.");
                return;
            }

            /* convert Connector list to Map of SeqJobRequest
            **  Each Connector in the list has:
            **      primary key = connectorId
            **      secondary elements = dbUrl, user, pswd, schema, spaceIds, tableNames
            **  Problem is:
            **      We can't distribute connector to individual thread, because there can be multiple connectors
            **      pointing to the same SpaceDB database (i.e. dbUrl), whereas we need one thread per SpaceDB instance.
            **  Hence, we transform the list to a Map with:
            **      primary key = dbUrl
            **      SeqJobRequest = user, pswd, combined(spaceIds), combined(tableNames)
            **  This ensures that each SeqJobRequest works individually on every SpaceDB instance
            */
            final Map<String, SeqJobRequest> jobRequestMap = new HashMap<>();
            for (final ConnectorDTO connector : connectorList) {
                SeqJobRequest jobReq = jobRequestMap.get(connector.getDbUrl());
                if (jobReq==null) {
                    jobReq = new SeqJobRequest();
                }
                jobReq.copyConnectorDTO(connector);
                jobRequestMap.put(connector.getDbUrl(), jobReq);
            }
            logger.debug("{} sequencer requests to be processed.", jobRequestMap.size());
            // Distribute SeqJobRequest's amongst thread pool to perform parallel sequencing (configurable poolSize e.g. 10 threads)
            distributeSeqJobRequestProcessing(jobRequestMap);
            logger.debug("All sequencer requests processed");
        }
        catch (Exception ex) {
            logger.error("Exception while running Sequencer job. ", ex);
        }
        logger.debug("Sequencer job finished!");
    }



    // Blocking function which distributes requests to a thread pool and waits for all threads to complete
    private void distributeSeqJobRequestProcessing(Map<String, SeqJobRequest> jobRequestMap) throws InterruptedException, ExecutionException {
        // create thread pool (if doesn't exist already)
        if (connHandlingPool == null) {
            connHandlingPool = new ThreadPoolExecutor(pubCfg.TXN_SEQ_TPOOL_CORE_SIZE,
                    pubCfg.TXN_SEQ_TPOOL_MAX_SIZE,
                    pubCfg.TXN_SEQ_TPOOL_KEEP_ALIVE_SEC,
                    TimeUnit.SECONDS,
                    new SynchronousQueue<>(), // queue with zero capacity
                    new ThreadPoolExecutor.CallerRunsPolicy()); // on reaching queue limit, caller thread itself is used for execution
        }

        // distribute SeqJobRequest's to thread pool
        final Set<String> dbUrlSet = jobRequestMap.keySet();
        final List<Future> fList = new ArrayList<Future>(dbUrlSet.size());
        for (final String dbUrl : dbUrlSet) {
            logger.debug("SeqJob entry to be submitted to thread for DB : {}", dbUrl);
            final Future f = connHandlingPool.submit(new SeqJobRequestHandler(adminDBConnParams, jobRequestMap.get(dbUrl)));
            fList.add(f);
        }
        // NOTE : We should not wait for completion of all threads, otherwise one buzy/long thread
        // can hold up restart of the entire job (thereby delaying other sequencer jobs as well)
        /*
        for (Future f : fList) {
            f.get();
        }*/
    }

}
