package com.here.xyz.events;

import java.util.HashMap;
import java.util.Map;

public class TrustedParams extends HashMap<String, Object> {

  public static final String COOKIES = "cookies";
  public static final String HEADERS = "headers";
  public static final String QUERY_PARAMS = "queryParams";

  public Map<String, String> getCookies() {
    //noinspection unchecked
    return (Map<String, String>) get(COOKIES);
  }

  public void setCookies(Map<String, String> cookies) {
    if (cookies == null) {
      return;
    }
    put(COOKIES, cookies);
  }

  public void putCookie(String name, String value) {
    if (!containsKey(COOKIES)) {
      put(COOKIES, new HashMap<String, String>());
    }
    getCookies().put(name, value);
  }

  public String getCookie(String name) {
    if (containsKey(COOKIES)) {
      return getCookies().get(name);
    }
    return null;
  }

  public Map<String, String> getHeaders() {
    //noinspection unchecked
    return (Map<String, String>) get(HEADERS);
  }

  public void setHeaders(Map<String, String> headers) {
    if (headers == null) {
      return;
    }
    put(HEADERS, headers);
  }

  public void putHeader(String name, String value) {
    if (!containsKey(HEADERS)) {
      put(HEADERS, new HashMap<String, String>());
    }
    getHeaders().put(name, value);
  }

  public String getHeader(String name) {
    if (containsKey(HEADERS)) {
      return getHeaders().get(name);
    }
    return null;
  }

  public Map<String, String> getQueryParams() {
    //noinspection unchecked
    return (Map<String, String>) get(QUERY_PARAMS);
  }

  public void setQueryParams(Map<String, String> queryParams) {
    if (queryParams == null) {
      return;
    }
    put(QUERY_PARAMS, queryParams);
  }

  public void putQueryParam(String name, String value) {
    if (!containsKey(QUERY_PARAMS)) {
      put(QUERY_PARAMS, new HashMap<String, String>());
    }
    getQueryParams().put(name, value);
  }

  public String getQueryParam(String name) {
    if (containsKey(QUERY_PARAMS)) {
      return getQueryParams().get(name);
    }
    return null;
  }
}
