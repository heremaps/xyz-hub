package com.here.xyz.pub.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PublisherJob implements Runnable {
    private static final Logger logger = LogManager.getLogger();

    // Called once per job execution to fetch and process all "active" subscriptions
    @Override
    public void run() {
        logger.info("PublisherJob got called");
        // TODO : Fetch all active subscriptions from AdminDB::xyz_config::xyz_subscription table

        // TODO : Distribute subscriptions amongst thread pool to perform parallel publish (configurable poolSize e.g. 10 threads)
    }

}
