package com.here.xyz;

// See: https://www.baeldung.com/jackson-json-view-annotation

/**
 * All views.
 */
public class View {

  /**
   * Included in all views.
   */
  public static class All {}

  /**
   * Exposed via REST API.
   */
  public static final class Public extends All {}

  /**
   * Exposed via REST API for owners only.
   */
  public static final class Protected extends All {}

  /**
   * Used internally for storage in a database or while sending to microservices, processors and alike.
   */
  public static final class Private extends All {}
}