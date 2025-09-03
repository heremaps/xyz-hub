package com.here.xyz.jobs.steps.outputs;

import com.here.xyz.XyzSerializable;

public class SetSummary implements XyzSerializable {

  private String type = "OutputSet";
  private long itemCount;
  private long byteSize;

  public String getType() {
    return type;
  }

  public SetSummary setType(String type) {
    this.type = type;
    return this;
  }

  public SetSummary withType(String type) {
    return setType(type);
  }

  public long getItemCount() {
    return itemCount;
  }

  public SetSummary setItemCount(long itemCount) {
    this.itemCount = itemCount;
    return this;
  }

  public SetSummary withItemCount(long itemCount) {
    return setItemCount(itemCount);
  }

  public long getByteSize() {
    return byteSize;
  }

  public SetSummary setByteSize(long byteSize) {
    this.byteSize = byteSize;
    return this;
  }

  public SetSummary withByteSize(long byteSize) {
    return setByteSize(byteSize);
  }
}
