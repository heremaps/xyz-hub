package com.here.xyz.pub.models;


// POJO for holding Transaction Data which is to be published
public class PubTransactionData {
    private long txnId;
    private long txnRecId;
    private String action;
    private String jsonData;
    private String featureId;

    public long getTxnId() {
        return txnId;
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    public long getTxnRecId() {
        return txnRecId;
    }

    public void setTxnRecId(long txnRecId) {
        this.txnRecId = txnRecId;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getJsonData() {
        return jsonData;
    }

    public void setJsonData(String jsonData) {
        this.jsonData = jsonData;
    }

    public String getFeatureId() {
        return featureId;
    }

    public void setFeatureId(String featureId) {
        this.featureId = featureId;
    }

    @Override
    public String toString() {
        return "PubTransactionData{" +
                "txnId=" + txnId +
                ", txnRecId=" + txnRecId +
                ", action='" + action + '\'' +
                ", jsonData='" + jsonData + '\'' +
                ", featureId='" + featureId + '\'' +
                '}';
    }

}
