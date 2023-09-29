package com.here.xyz.httpconnector.util.jobs.datasets;

import com.here.xyz.httpconnector.util.jobs.Export.Filters;

public interface FilteringSource<T extends FilteringSource> {

  Filters getFilters();

  void setFilters(Filters filters);

  T withFilters(Filters filters);
}
