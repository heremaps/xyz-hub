package com.here.xyz.pub.models;


// POJO for holding Transaction Data which is to be published
public class PubTransactionData {
    private long txnId;
    private String action;
    private String jsonData;

    public long getTxnId() {
        return txnId;
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
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

    @Override
    public String toString() {
        return "PubTransactionData{" +
                "txnId='" + txnId + '\'' +
                ", action='" + action + '\'' +
                ", jsonData='" + jsonData + '\'' +
                '}';
    }
}
