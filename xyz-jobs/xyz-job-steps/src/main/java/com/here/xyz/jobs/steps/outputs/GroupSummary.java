package com.here.xyz.jobs.steps.outputs;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.here.xyz.XyzSerializable;
import java.util.Map;

@JsonInclude(NON_DEFAULT)
public class GroupSummary implements XyzSerializable {

  private String type = "OutputGroup";
  private Map<String, SetSummary> items;
  private long itemCount;
  private long byteSize;

  public String getType() {
    return type;
  }

  public GroupSummary setType(String type) {
    this.type = type;
    return this;
  }

  public GroupSummary withType(String type) {
    return setType(type);
  }

  public Map<String, SetSummary> getItems() {
    return items;
  }

  public GroupSummary setItems(Map<String, SetSummary> items) {
    this.items = items;
    return this;
  }

  public GroupSummary withItems(Map<String, SetSummary> items) {
    return setItems(items);
  }

  public long getItemCount() {
    return itemCount;
  }

  public GroupSummary setItemCount(long itemCount) {
    this.itemCount = itemCount;
    return this;
  }

  public GroupSummary withItemCount(long itemCount) {
    return setItemCount(itemCount);
  }

  public long getByteSize() {
    return byteSize;
  }

  public GroupSummary setByteSize(long byteSize) {
    this.byteSize = byteSize;
    return this;
  }

  public GroupSummary withByteSize(long byteSize) {
    return setByteSize(byteSize);
  }
}
