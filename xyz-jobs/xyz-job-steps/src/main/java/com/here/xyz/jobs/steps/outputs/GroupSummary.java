package com.here.xyz.jobs.steps.outputs;

import com.here.xyz.XyzSerializable;
import java.util.Map;

public class GroupSummary implements XyzSerializable {

  private String type = "OutputGroup";
  private Map<String, SetSummary> items;
  private long itemCount;
  private long byteSize;

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public GroupSummary withType(String type) {
    this.type = type;
    return this;
  }

  public Map<String, SetSummary> getItems() {
    return items;
  }

  public void setItems(Map<String, SetSummary> items) {
    this.items = items;
  }

  public GroupSummary withItems(Map<String, SetSummary> items) {
    this.items = items;
    return this;
  }

  public long getItemCount() {
    return itemCount;
  }

  public void setItemCount(long itemCount) {
    this.itemCount = itemCount;
  }

  public GroupSummary withItemCount(long itemCount) {
    this.itemCount = itemCount;
    return this;
  }

  public long getByteSize() {
    return byteSize;
  }

  public void setByteSize(long byteSize) {
    this.byteSize = byteSize;
  }

  public GroupSummary withByteSize(long byteSize) {
    this.byteSize = byteSize;
    return this;
  }
}
