package com.here.xyz.pub.impl;

import com.here.xyz.models.hub.Subscription;
import com.here.xyz.pub.models.PubConfig;
import com.here.xyz.pub.models.PubTransactionData;
import com.here.xyz.pub.models.PublishEntryDTO;
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
    public PublishEntryDTO publishTransactions(final PubConfig pubCfg, final Subscription sub,
                                               final List<PubTransactionData> txnList,
                                               long lastStoredTxnId, long lastStoredTxnRecId) throws Exception {
        final String subId = sub.getId();
        final String spaceId = sub.getSource();
        final String snsTopic = PubUtil.getSnsTopicARN(sub);
        final long lotStartTS = System.currentTimeMillis();
        // local counters
        final PublishEntryDTO pubDTO = new PublishEntryDTO(lastStoredTxnId, lastStoredTxnRecId);
        long publishedRecCnt = 0;

        try {
            // Publish all transactions on SNS Topic (in the same order they were fetched)
            for (final PubTransactionData txnData : txnList) {
                final long crtTxnId = txnData.getTxnId();
                final long crtTxnRecId = txnData.getTxnRecId();
                final long startTS = System.currentTimeMillis();
                // Convert transaction payload into expected publishable format
                final String pubFormat = MessageUtil.getMsgMapperInstance(sub).mapToPublishableFormat(sub, txnData);

                // Prepare SNS Notification message
                final String msg = MessageUtil.compressAndEncodeToString(pubFormat);
                final Map<String, MessageAttributeValue> msgAttrMap = new HashMap<>();
                MessageUtil.addToAttributeMap(msgAttrMap, "action", txnData.getAction());
                MessageUtil.addToAttributeMap(msgAttrMap, "space", spaceId);
                MessageUtil.addToAttributeMap(msgAttrMap, "featureId", txnData.getFeatureId());
                // Add other custom attributes
                MessageUtil.addCustomFieldsToAttributeMap(msgAttrMap, sub, txnData.getJsonData());

                // Publish message to SNS Topic
                // TODO : Support multi-region based on subscription configuration.
                // We may require region specific publisher job for the respective subscriptions.
                final SnsAsyncClient snsClient = AwsUtil.getSnsAsyncClient(pubCfg.AWS_DEFAULT_REGION);
                final PublishRequest request = PublishRequest.builder()
                        .message(msg)
                        .messageAttributes(msgAttrMap)
                        .topicArn(snsTopic)
                        .build();
                final CompletableFuture<PublishResponse> futureResponse = snsClient.publish(request);
                final PublishResponse result = futureResponse.join();
                publishedRecCnt++;
                final long timeTaken = System.currentTimeMillis() - startTS;
                logger.debug("Message no. [{}], txnId={}, txnRecId={}, published in {}ms to SNS [{}] for subId [{}]. Status is {}.",
                        publishedRecCnt, crtTxnId, crtTxnRecId, timeTaken, snsTopic, subId, result.sdkHttpResponse().statusCode());

                // Record last successfully published transaction Id's
                pubDTO.setLastTxnId(crtTxnId);
                pubDTO.setLastTxnRecId(crtTxnRecId);
            }
        }
        finally {
            final long lotTimeTaken = System.currentTimeMillis() - lotStartTS;
            logger.info("Published [{}] records in {}ms to SNS [{}] for subId [{}], space [{}]. Last published txn was [{}]",
                    publishedRecCnt, lotTimeTaken, snsTopic, subId, spaceId, pubDTO);
        }

        return pubDTO;
    }


}
