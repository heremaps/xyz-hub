package com.here.xyz.util.pagination;

import java.util.List;

public interface PagedDataRetriever<T, P> {

  List<T> getItems(P params);

  Page<T> getPage(P params, int pageSize, String pageToken);

  default Page<T> getFirstPage(P params) {
    return getPage(params, getDefaultPageSize(), null);
  }

  default int getDefaultPageSize() {
    return 30000;
  }

  default long count(P params) {
    return getFirstPage(params).getTotalItems();
  }
}
