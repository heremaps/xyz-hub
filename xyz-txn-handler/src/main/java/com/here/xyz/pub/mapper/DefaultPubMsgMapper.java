package com.here.xyz.pub.mapper;

import com.here.xyz.models.hub.Subscription;
import com.here.xyz.pub.models.PubTransactionData;
import com.here.xyz.pub.util.MessageUtil;
import java.util.HashMap;
import java.util.Map;

public class DefaultPubMsgMapper implements IPubMsgMapper {

    public String mapToPublishableFormat(final Subscription sub, final PubTransactionData txnData) {
        final Map<String, Object> jsonDataMap = MessageUtil.fromJson(txnData.getJsonData(), Map.class);
        final String action = txnData.getAction();
        // prepare final message map
        final Map<String, Object> msgMap = new HashMap<>();
        msgMap.put("action", action);
        msgMap.put("space", sub.getSource());
        // TODO : populate featureId
        if ("DELETE".equalsIgnoreCase(action)) {
            msgMap.put("oldValuesMap", jsonDataMap);
        }
        else {
            msgMap.put("feature", jsonDataMap);
        }
        // return converted message in string
        return MessageUtil.toJson(msgMap);
    }

}
