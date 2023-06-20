package com.here.xyz.models.payload.events;

/**
 * The query delimiter type as defined in <a href="https://datatracker.ietf.org/doc/html/rfc3986#section-2.2">RFC-3986</a>. Note that
 * technically all characters that are not explicitly unreserved, maybe used as delimiters, which means all characters not being ALPHA /
 * DIGIT / "-" / "." / "_" / "~". However, it is recommended to only use the reserved characters as delimiters to guarantee that standard
 * encoding algorithms work as intended.
 */
public enum QueryDelimiterType {
  /**
   * A general delimiter that is being used as delimiters of the generic URI components.
   */
  GENERAL,
  /**
   * A sub-delimiter.
   */
  SUB,
  /**
   * An unsafe delimiter, characters not being unreserved, but neither reserved for the delimiter purpose, for example ">".
   */
  UNSAFE;
}