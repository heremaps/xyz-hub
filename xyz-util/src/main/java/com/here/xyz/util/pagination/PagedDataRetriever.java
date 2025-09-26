package com.here.xyz.util.pagination;

import java.util.List;

public interface PagedDataRetriever<R, P> {

  List<R> getItems(P params);

  Page<R> getPage(P params, int pageSize, String pageToken);
}
