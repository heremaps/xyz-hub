package com.here.naksha.lib.core.util;


import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** An annotation to read a property from a specific environment variable. */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE})
@Repeatable(EnvNames.class)
public @interface EnvName {

    /**
     * The name of the environment variable.
     *
     * @return name of the environment variable.
     */
    String value() default "";

    /**
     * If the name should be prefixed.
     *
     * @return if the name should be prefixed.
     */
    boolean prefix() default false;
}
