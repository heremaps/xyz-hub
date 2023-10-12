package com.here.xyz.httpconnector.util.jobs.datasets;

import com.here.xyz.httpconnector.util.jobs.Export.Filters;
import com.here.xyz.httpconnector.util.jobs.datasets.DatasetDescription.Space;
import java.util.List;
import java.util.stream.Collectors;

public class Spaces<T extends Spaces> extends DatasetDescription implements FilteringSource<T>, CombinedDatasetDescription<Space> {

  private List<String> spaceIds;
  private Filters filters;

  public List<String> getSpaceIds() {
    return spaceIds;
  }

  public void setSpaceIds(List<String> spaceIds) {
    this.spaceIds = spaceIds;
  }

  public com.here.xyz.httpconnector.util.jobs.datasets.Spaces withSpaceIds(List<String> spaceIds) {
    setSpaceIds(spaceIds);
    return this;
  }

  @Override
  public Filters getFilters() {
    return filters;
  }

  @Override
  public void setFilters(Filters filters) {
    this.filters = filters;
  }

  @Override
  public T withFilters(Filters filters) {
    setFilters(filters);
    return (T) this;
  }

  public String getKey() {
    return String.join(",", spaceIds);
  }

  @Override
  public List<Space> createChildEntities() {
    return getSpaceIds()
        .stream()
        .map(spaceId -> (Space) new Space().withFilters(getFilters()).withId(spaceId))
        .collect(Collectors.toList());
  }
}
