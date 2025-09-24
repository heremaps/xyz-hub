package com.here.xyz.jobs.steps.outputs;

import com.here.xyz.XyzSerializable;
import java.util.Map;

public class GroupSummary implements XyzSerializable {

  public static final String OUTPUT_TYPE = "OutputGroup";
  public static final String INPUT_TYPE = "InputGroup";

  private Map<String, SetSummary> sets;
  private String type;
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

  public Map<String, SetSummary> getSets() {
    return sets;
  }

  public void setSets(Map<String, SetSummary> sets) {
    this.sets = sets;
  }

  public GroupSummary withItems(Map<String, SetSummary> items) {
    this.sets = items;
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
