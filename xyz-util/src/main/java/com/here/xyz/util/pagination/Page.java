package com.here.xyz.util.pagination;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.here.xyz.XyzSerializable;
import java.io.Serializable;
import java.util.List;

public class Page<T> implements Serializable, XyzSerializable {

  @JsonInclude(Include.NON_NULL)
  private List<T> items;

  @JsonInclude(Include.NON_NULL)
  private String nextPageToken;

  @JsonInclude(Include.NON_NULL)
  private long totalItems;

  public Page() {
  }

  public Page(List<T> items, String nextPageToken, long totalItems) {
    this.items = items;
    this.nextPageToken = nextPageToken;
    this.totalItems = totalItems;
  }

  public List<T> getItems() {
    return items;
  }

  public Page<T> setItems(List<T> items) {
    this.items = items;
    return this;
  }

  public String getNextPageToken() {
    return nextPageToken;
  }

  public Page<T> setNextPageToken(String nextPageToken) {
    this.nextPageToken = nextPageToken;
    return this;
  }

  public long getTotalItems() {
    return totalItems;
  }

  public Page<T> setTotalItems(long totalItems) {
    this.totalItems = totalItems;
    return this;
  }

  public int size() {
    return items != null ? items.size() : 0;
  }
}
