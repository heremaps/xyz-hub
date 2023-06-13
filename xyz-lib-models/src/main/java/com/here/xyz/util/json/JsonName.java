package com.here.xyz.util.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation to read a property from a JSON object. This is defined as a name alias, so the
 * property is read by its real name and then the alternatives are tried.
 *
 * To rename the property itself, please use the {@link JsonProperty} annotation.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@Repeatable(JsonNames.class)
public @interface JsonName {

  /**
   * The name of the property in the JSON.
   *
   * @return name of the property in the JSON.
   */
  String value() default "";
}
