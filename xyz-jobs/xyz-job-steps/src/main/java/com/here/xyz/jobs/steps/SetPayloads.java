package com.here.xyz.jobs.steps;

import com.here.xyz.XyzSerializable;

public class SetPayloads implements XyzSerializable {

  private long itemCount;
  private long byteSize;

  public long getItemCount() {
    return itemCount;
  }

  public SetPayloads setItemCount(long itemCount) {
    this.itemCount = itemCount;
    return this;
  }

  public SetPayloads withItemCount(long itemCount) {
    return setItemCount(itemCount);
  }

  public long getByteSize() {
    return byteSize;
  }

  public SetPayloads setByteSize(long byteSize) {
    this.byteSize = byteSize;
    return this;
  }

  public SetPayloads withByteSize(long byteSize) {
    return setByteSize(byteSize);
  }
}
