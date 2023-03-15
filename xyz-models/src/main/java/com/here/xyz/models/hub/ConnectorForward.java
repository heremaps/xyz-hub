package com.here.xyz.models.hub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectorForward {

  @JsonProperty
  public List<String> cookies;

  @JsonProperty
  public List<String> headers;
  @JsonProperty
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
}
