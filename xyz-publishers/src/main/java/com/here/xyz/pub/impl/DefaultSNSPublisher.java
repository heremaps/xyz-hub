package com.here.xyz.pub.impl;

import com.here.xyz.models.hub.Subscription;
import com.here.xyz.pub.models.PubTransactionData;
import com.here.xyz.pub.util.AwsUtil;
import com.here.xyz.pub.util.MessageUtil;
import com.here.xyz.pub.util.PubUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class DefaultSNSPublisher implements IPublisher {
    private static final Logger logger = LogManager.getLogger();

    // Convert and publish transactions to desired SNS Topic
    @Override
    public long publishTransactions(final Subscription sub,
            final List<PubTransactionData> txnList, long lastStoredTxnId) {
        final String subId = sub.getId();
        final String spaceId = sub.getSource();
        final String snsTopic = PubUtil.getSnsTopicARN(sub);
        final long lotStartTS = System.currentTimeMillis();
        // local counters
        long crtTxnId = 0;
        long prevTxnId = lastStoredTxnId;
        // final counter to be returned
        long lastCompletedTxnId = prevTxnId;

        try {
            // Publish all transactions on SNS Topic (in the same order they were fetched)
            for (final PubTransactionData txnData : txnList) {
                crtTxnId = txnData.getTxnId();
                final long startTS = System.currentTimeMillis();
                // Convert transaction payload into expected publishable format
                final String pubFormat = MessageUtil.getMsgMapperInstance(sub).mapToPublishableFormat(sub, txnData);

                // Prepare SNS Notification message
                final String msg = MessageUtil.compressAndEncodeToString(pubFormat);
                final Map<String, MessageAttributeValue> msgAttrMap = new HashMap<>();
                // TODO : Add featureId, featureType, status (properties.status)
                MessageUtil.addtoAttributeMap(msgAttrMap, "action", txnData.getAction());
                MessageUtil.addtoAttributeMap(msgAttrMap, "space", spaceId);

                // Publish message to SNS Topic
                // TODO : Avoid hardcoded region and obtain it from configuration
                final SnsAsyncClient snsClient = AwsUtil.getSnsAsyncClient("us-east-1");
                final PublishRequest request = PublishRequest.builder()
                        .message(msg)
                        .messageAttributes(msgAttrMap)
                        .topicArn(snsTopic)
                        .build();
                final CompletableFuture<PublishResponse> futureResponse = snsClient.publish(request);
                final PublishResponse result = futureResponse.join();
                final long timeTaken = System.currentTimeMillis() - startTS;
                // TODO : Debug
                logger.info("Message [{}] published in {}ms to SNS [{}] for subId [{}]. Status is {}.",
                        crtTxnId, timeTaken, snsTopic, subId, result.sdkHttpResponse().statusCode());

                // Record last successfully published txn_id
                // NOTE : Same txn_id can have multiple messages,
                //      hence we need to check if all messages with prevTxnId got published or not
                //      and then only move the counter
                if (crtTxnId != prevTxnId) {
                    lastCompletedTxnId = prevTxnId;
                    prevTxnId = crtTxnId;
                }
            }
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            lastCompletedTxnId = prevTxnId;
            final long lotTimeTaken = System.currentTimeMillis() - lotStartTS;
            logger.info("Published [{}] records in {}ms to SNS [{}] for subId [{}], space [{}]. Last published txnId was [{}]",
                    txnList.size(), lotTimeTaken, snsTopic, subId, spaceId, lastCompletedTxnId);
        }

        return lastCompletedTxnId;
    }


}
