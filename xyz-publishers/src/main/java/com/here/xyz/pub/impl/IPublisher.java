package com.here.xyz.pub.impl;

import com.here.xyz.models.hub.Subscription;
import com.here.xyz.pub.models.PubTransactionData;

import java.util.List;

public interface IPublisher {
    long publishTransactions(final Subscription sub, final List<PubTransactionData> txnList,
                                    final long crtTxnId);

}
