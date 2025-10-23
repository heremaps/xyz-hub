package com.here.xyz.util.pagination;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.here.xyz.XyzSerializable;
import java.util.ArrayList;
import java.util.List;

public class Page<T> implements XyzSerializable {

  @JsonInclude(Include.NON_NULL)
  private List<T> items;

  @JsonInclude(Include.NON_NULL)
  private String nextPageToken;

  public Page() {
    this.items = new ArrayList<>();
  }

  public Page(List<T> items) {
    this.items = items;
  }

  public Page(List<T> items, String nextPageToken) {
    this.items = items;
    this.nextPageToken = nextPageToken;
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

  public int size() {
    return items != null ? items.size() : 0;
  }
}
