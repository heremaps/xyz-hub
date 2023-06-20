package com.here.xyz.pub.mapper;

import com.here.naksha.lib.core.models.hub.Subscription;
import com.here.xyz.pub.models.PubTransactionData;

public interface IPubMsgMapper {

    String mapToPublishableFormat(final Subscription sub, final PubTransactionData txnData);
}
