package com.here.xyz.models.hub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.View.All;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectorForward {

  @JsonProperty
  @JsonView(All.class)
  public List<String> cookies;

  @JsonProperty
  @JsonView(All.class)
  public List<String> headers;

  @JsonProperty
  @JsonView(All.class)
  public List<String> queryParams;

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConnectorForward that = (ConnectorForward) o;
    return Objects.equals(cookies, that.cookies) &&
        Objects.equals(headers, that.headers) &&
        Objects.equals(queryParams, that.queryParams);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cookies, headers, queryParams);
  }
}