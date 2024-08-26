package com.here.xyz.util.service.aws.dynamo;

public class IndexDefinition {

    private final String hashKey;
    private final String rangeKey;

    public IndexDefinition(String hashKey) {
        this.hashKey = hashKey;
        this.rangeKey = null;
    }

    public IndexDefinition(String hashKey, String rangeKey) {
        this.hashKey = hashKey;
        this.rangeKey = rangeKey;
    }

    public String getHashKey() {
        return hashKey;
    }

    public String getRangeKey() {
        return rangeKey;
    }
}
