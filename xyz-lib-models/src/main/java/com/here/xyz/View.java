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
   * Pure public properties are only available to externals, so only exposed via REST API (mainly virtual read-only).
   */
  public static class Public extends All {}

  /**
   * Exposed via REST API, limited to owners only.
   */
  public static class Protected extends All {}

  /**
   * Used internally for storage in a database or while sending to microservices, processors and alike.
   */
  public static class Private extends Protected {}
}
