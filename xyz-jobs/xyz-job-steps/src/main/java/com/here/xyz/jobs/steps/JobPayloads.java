package com.here.xyz.jobs.steps;

import com.here.xyz.XyzSerializable;
import java.util.Map;

public class JobPayloads implements XyzSerializable {

  private Map<String, GroupPayloads> groups;
  private long itemCount;
  private long byteSize;

  public Map<String, GroupPayloads> getGroups() {
    return groups;
  }

  public JobPayloads setGroups(Map<String, GroupPayloads> groups) {
    this.groups = groups;
    return this;
  }

  public JobPayloads withGroups(Map<String, GroupPayloads> items) {
    return setGroups(items);
  }

  public long getItemCount() {
    return itemCount;
  }

  public JobPayloads setItemCount(long itemCount) {
    this.itemCount = itemCount;
    return this;
  }

  public JobPayloads withItemCount(long itemCount) {
    return setItemCount(itemCount);
  }

  public long getByteSize() {
    return byteSize;
  }

  public JobPayloads setByteSize(long byteSize) {
    this.byteSize = byteSize;
    return this;
  }

  public JobPayloads withByteSize(long byteSize) {
    return setByteSize(byteSize);
  }
}
