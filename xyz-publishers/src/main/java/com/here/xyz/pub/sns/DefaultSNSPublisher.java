package com.here.xyz.pub.sns;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DefaultSNSPublisher implements IPublisher {
    private static final Logger logger = LogManager.getLogger();

    // Convert and publish transactions to desired SNS Topic
    public static long publishTransactions(
            // final List<TransactionPayload> txns,
            long crtTxnId
            ) {
        long lastTxnId = crtTxnId;

        // TODO : Convert each transaction payload into expected Notification format

        // TODO : Obtain AWS SNS Client

        // TODO : Publish all notifications on SNS Topic (in the same order they were fetched)

        // TODO : Return last successfully published txn_id

        return lastTxnId;
    }
}
