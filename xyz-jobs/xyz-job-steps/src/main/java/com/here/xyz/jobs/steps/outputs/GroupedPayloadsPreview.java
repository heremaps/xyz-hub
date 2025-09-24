package com.here.xyz.jobs.steps.outputs;

import com.here.xyz.XyzSerializable;
import java.util.Map;

public class GroupedPayloadsPreview implements XyzSerializable {

  public static final String OUTPUT_TYPE = "JobOutputs";
  public static final String INPUT_TYPE = "JobInputs";

  private String type;
  private Map<String, GroupSummary> groups;
  private long itemCount;
  private long byteSize;

  public String getType() {
    return type;
  }

  public GroupedPayloadsPreview setType(String type) {
    this.type = type;
    return this;
  }

  public GroupedPayloadsPreview withType(String type) {
    return setType(type);
  }

  public Map<String, GroupSummary> getGroups() {
    return groups;
  }

  public GroupedPayloadsPreview setGroups(Map<String, GroupSummary> groups) {
    this.groups = groups;
    return this;
  }

  public GroupedPayloadsPreview withItems(Map<String, GroupSummary> items) {
    return setGroups(items);
  }

  public long getItemCount() {
    return itemCount;
  }

  public GroupedPayloadsPreview setItemCount(long itemCount) {
    this.itemCount = itemCount;
    return this;
  }

  public GroupedPayloadsPreview withItemCount(long itemCount) {
    return setItemCount(itemCount);
  }

  public long getByteSize() {
    return byteSize;
  }

  public GroupedPayloadsPreview setByteSize(long byteSize) {
    this.byteSize = byteSize;
    return this;
  }

  public GroupedPayloadsPreview withByteSize(long byteSize) {
    return setByteSize(byteSize);
  }
}
