package com.here.naksha.lib.core.models.payload.responses;

import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.responses.StatisticsResponse.Value;
import java.util.Map;

public class StorageStatistics extends XyzResponse {

  private Map<String, SpaceByteSizes> byteSizes;
  private long createdAt;

  /**
   * @return A map of which the keys are the space IDs and the values are the according byte size
   *     information of the according space.
   */
  public Map<String, SpaceByteSizes> getByteSizes() {
    return byteSizes;
  }

  public void setByteSizes(Map<String, SpaceByteSizes> byteSizes) {
    this.byteSizes = byteSizes;
  }

  public StorageStatistics withByteSizes(Map<String, SpaceByteSizes> byteSizes) {
    setByteSizes(byteSizes);
    return this;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }

  public StorageStatistics withCreatedAt(long createdAt) {
    setCreatedAt(createdAt);
    return this;
  }

  public static class SpaceByteSizes {
    private StatisticsResponse.Value<Long> contentBytes;
    private StatisticsResponse.Value<Long> historyBytes;
    private StatisticsResponse.Value<Long> searchablePropertiesBytes;
    private String error;

    public Value<Long> getContentBytes() {
      return contentBytes;
    }

    public void setContentBytes(Value<Long> contentBytes) {
      this.contentBytes = contentBytes;
    }

    public SpaceByteSizes withContentBytes(Value<Long> contentBytes) {
      setContentBytes(contentBytes);
      return this;
    }

    public Value<Long> getHistoryBytes() {
      return historyBytes;
    }

    public void setHistoryBytes(Value<Long> historyBytes) {
      this.historyBytes = historyBytes;
    }

    public SpaceByteSizes withHistoryBytes(Value<Long> historyBytes) {
      setHistoryBytes(historyBytes);
      return this;
    }

    public Value<Long> getSearchablePropertiesBytes() {
      return searchablePropertiesBytes;
    }

    public void setSearchablePropertiesBytes(Value<Long> searchablePropertiesBytes) {
      this.searchablePropertiesBytes = searchablePropertiesBytes;
    }

    public SpaceByteSizes withSearchablePropertiesBytes(Value<Long> searchablePropertiesBytes) {
      setSearchablePropertiesBytes(searchablePropertiesBytes);
      return this;
    }

    public String getError() {
      return error;
    }

    public void setError(String error) {
      this.error = error;
    }

    public SpaceByteSizes withError(String error) {
      setError(error);
      return this;
    }
  }
}
