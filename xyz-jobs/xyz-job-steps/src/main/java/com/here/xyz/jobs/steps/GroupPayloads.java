package com.here.xyz.jobs.steps;

import com.here.xyz.XyzSerializable;
import java.util.Map;

public class GroupPayloads implements XyzSerializable {

  private Map<String, SetPayloads> sets;
  private long itemCount;
  private long byteSize;

  public Map<String, SetPayloads> getSets() {
    return sets;
  }

  public void setSets(Map<String, SetPayloads> sets) {
    this.sets = sets;
  }

  public GroupPayloads withSets(Map<String, SetPayloads> items) {
    this.sets = items;
    return this;
  }

  public long getItemCount() {
    return itemCount;
  }

  public void setItemCount(long itemCount) {
    this.itemCount = itemCount;
  }

  public GroupPayloads withItemCount(long itemCount) {
    this.itemCount = itemCount;
    return this;
  }

  public long getByteSize() {
    return byteSize;
  }

  public void setByteSize(long byteSize) {
    this.byteSize = byteSize;
  }

  public GroupPayloads withByteSize(long byteSize) {
    this.byteSize = byteSize;
    return this;
  }
}
