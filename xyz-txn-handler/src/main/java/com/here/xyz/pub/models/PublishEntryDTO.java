package com.here.xyz.pub.models;


// POJO for holding Transaction Publish details
public class PublishEntryDTO {
    // One transaction can comprise of multiple records.
    // lastTxnId indicates the last transaction id which was processed
    // lastTxnRecId indicates the last record within a transaction id which was published
    private long lastTxnId;
    private long lastTxnRecId;

    public PublishEntryDTO() {
    }

    public PublishEntryDTO(final long lastTxnId, final long lastTxnRecId) {
        this.lastTxnId = lastTxnId;
        this.lastTxnRecId = lastTxnRecId;
    }

    public long getLastTxnId() {
        return lastTxnId;
    }

    public void setLastTxnId(long lastTxnId) {
        this.lastTxnId = lastTxnId;
    }

    public long getLastTxnRecId() {
        return lastTxnRecId;
    }

    public void setLastTxnRecId(long lastTxnRecId) {
        this.lastTxnRecId = lastTxnRecId;
    }

    @Override
    public String toString() {
        return "{" +
                "lastTxnId=" + lastTxnId +
                ", lastTxnRecId=" + lastTxnRecId +
                '}';
    }

}
