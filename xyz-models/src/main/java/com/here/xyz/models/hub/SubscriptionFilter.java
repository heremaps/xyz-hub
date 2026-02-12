package com.here.xyz.models.hub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.here.xyz.models.filters.SpatialFilter;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SubscriptionFilter {
  private List<String> jsonPaths;
  private SpatialFilter spatialFilter;

  public List<String> getJsonPaths() {
    return jsonPaths;
  }

  public void setJsonPaths(List<String> jsonPaths) {
    this.jsonPaths = jsonPaths;
  }

  public SubscriptionFilter withJsonPaths(List<String> jsonPaths) {
    this.jsonPaths = jsonPaths;
    return this;
  }

  public SpatialFilter getSpatialFilter() {
    return spatialFilter;
  }

  public void setSpatialFilter(SpatialFilter spatialFilter) {
    this.spatialFilter = spatialFilter;
  }

  public SubscriptionFilter withSpatialFilter(SpatialFilter spatialFilter) {
    this.spatialFilter = spatialFilter;
    return this;
  }

}
