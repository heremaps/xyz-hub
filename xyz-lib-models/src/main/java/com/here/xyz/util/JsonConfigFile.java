package com.here.xyz.util;

import static com.here.xyz.util.IoHelp.readConfigFromHomeOrResource;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.util.IoHelp.LoadedConfig;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Base class of a configuration file that is read from a JSON file with optional override via environment variables. The environment
 * variables are all required to be upper-cased and are optionally prefixed by a defined string. The configuration file should contain the
 * member names in exact notation, unless annotated.
 *
 * @param <SELF> this type.
 */
@SuppressWarnings("unchecked")
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class JsonConfigFile<SELF extends JsonConfigFile<SELF>> extends FileOrResource<SELF> {

  protected JsonConfigFile(@NotNull String filename) {
    super(filename);
  }

  /**
   * Returns the name of the environment variable that should be used to define the search path for the configuration files.
   * <p>
   * Optionally it is possible to add an {@link EnvName} annotation to the class, then this defines the name of the environment variable
   * that holds the search path.
   * <p>
   * In a nutshell, by default the method returns {@code NAKSHA_CONFIG_PATH}, if the {@link EnvName} annotation is found at the given class
   * type, for example {@code @EnvName("MY_CONFIG_PATH")}, then the method returns exactly this: {@code MY_CONFIG_PATH}.
   *
   * @param classType the class for which to check the annotations.
   * @return the name of the environment variable that holds the config file search path.
   */
  public static @NotNull String configPathEnvName(@Nullable Class<?> classType) {
    while (classType != null) {
      if (classType.isAnnotationPresent(EnvName.class)) {
        final EnvName[] envNames = classType.getAnnotationsByType(EnvName.class);
        return envNames[0].value();
      }
      classType = classType.getSuperclass();
    }
    return "NAKSHA_CONFIG_PATH";
  }

  /**
   * An optional prefix to be placed in-front of all field names to be read from environment variables. So, the prefix "FOO_" will cause the
   * property “bar” to be looked-up as "FOO_BAR". If no prefix is returned, the environment variable name must explicitly be declared using
   * the annotation {@link EnvName}. Multiple names can be defined and combined with the prefix.
   *
   * @return a prefix or null.
   */
  protected @Nullable String envPrefix() {
    return null;
  }

  /**
   * A helper method to detect pseudo null values in environment variables. Without this, you can never undefine (unset) a pre-defined value
   * via environment variable. To allow this, this method treats an empty string and the string "null" as null (undefine).
   *
   * @param string the string that is read from the environment variable.
   * @return null if the value represents null; otherwise the given value.
   */
  public static @Nullable String nullable(@Nullable String string) {
    return string == null || string.isEmpty() || "null".equalsIgnoreCase(string) ? null : string;
  }

  /**
   * Used for Jackson to parse a JSON object into a map.
   */
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
  };

  /**
   * Returns the name of the application, needed to search for the default configuration location.
   *
   * @return The application name.
   */
  abstract protected @NotNull String appName();

  /**
   * Load the configuration from a disk file, a file in the resources (JAR) or from environment variables. Basically the order is to first
   * read from a disk file, if not found, read from resources. In both cases, eventually the environment variables will override all values
   * read.
   *
   * @return this.
   * @throws IOException if any error occurred while reading from disk or resources.
   */
  public final @NotNull SELF load() throws IOException {
    return load(System::getenv);
  }

  /**
   * Load the configuration from a disk file, a file in the resources (JAR) or from environment variables. Basically the order is to first
   * read from a disk file, if not found, read from resources. In both cases, eventually the environment variables will override all values
   * read.
   *
   * @param getEnv the reference to the method that reads environment variables.
   * @return this.
   * @throws IOException          if any error occurred while reading from disk or resources.
   * @throws NullPointerException if getEnv is null.
   */
  public final @NotNull SELF load(final @Nullable Function<@NotNull String, @Nullable String> getEnv) throws IOException {
    Map<String, Object> configValues = null;
    LoadedConfig loaded = null;
    try {
      loaded = readConfigFromHomeOrResource(filename(), false, appName(), searchPath());
      configValues = XyzSerializable.DEFAULT_MAPPER.get().readValue(loaded.bytes(), MAP_TYPE);
    } catch (Throwable t) {
      logger.error("Failed to load configuration file: {} (path={})", filename, loaded != null ? loaded.path() : "", t);
      loaded = null;
    }
    loadFromMap(configValues, getEnv);
    this.loadPath = loaded != null ? loaded.path() : null;
    return (SELF) this;
  }

  @JsonIgnore
  private String loadPath;

  /**
   * Return the path of the config file loaded; if any.
   *
   * @return The path of the config file loaded; if any.
   */
  public @Nullable String loadPath() {
    return loadPath;
  }

  /**
   * Read the configuration from the given map and eventually use environment variables to override values.
   *
   * @param configValues the configuration values to use.
   * @return this.
   */
  public final @NotNull SELF loadFromMap(@NotNull Map<String, Object> configValues) {
    return loadFromMap(configValues, System::getenv);
  }

  /**
   * Read the configuration from a given map and eventually use environment variables to override values.
   *
   * @param configValues the configuration values to use.
   * @param getEnv       the reference to the method that reads the environment variable; if {@code null}, then no environment variables
   *                     read.
   * @return this.
   */
  public final @NotNull SELF loadFromMap(
      @Nullable Map<@NotNull String, @Nullable Object> configValues,
      @Nullable Function<String, String> getEnv
  ) {
    this.loadPath = null;
    final String prefix;
    final int prefixLen;
    final StringBuilder sb;
    if (getEnv != null) {
      sb = new StringBuilder(128);
      prefix = envPrefix();
      if (prefix != null) {
        prefixLen = prefix.length();
        for (int i = 0; i < prefix.length(); i++) {
          sb.append(Character.toUpperCase(prefix.charAt(i)));
        }
      } else {
        prefixLen = 0;
      }
    } else {
      prefix = null;
      prefixLen = 0;
      sb = null;
    }
    Class<?> theClass = getClass();
    do {
      for (final Field field : theClass.getDeclaredFields()) {
        String name = field.getName();
        // @JsonProperty will rename the property.
        if (field.isAnnotationPresent(JsonProperty.class)) {
          name = field.getAnnotation(JsonProperty.class).value();
        }

        if (configValues != null) {
          // If the property exists under its correct name, use this.
          if (configValues.containsKey(name)) {
            setField(field, configValues.get(name));
          } else {
            // Try all alternative names in the order in which they are annotated.
            if (field.isAnnotationPresent(JsonName.class)) {
              final JsonName[] jsonNames = field.getAnnotationsByType(JsonName.class);
              for (final JsonName jsonName : jsonNames) {
                if (configValues.containsKey(jsonName.value())) {
                  setField(field, configValues.get(jsonName.value()));
                  break;
                }
              }
            }
          }
        }
        if (getEnv != null) {
          if (prefix != null) {
            setFromEnv(sb, prefixLen, field, name, true, getEnv);
          }
          if (field.isAnnotationPresent(EnvName.class)) {
            final EnvName[] envNames = field.getAnnotationsByType(EnvName.class);
            for (final EnvName envName : envNames) {
              setFromEnv(sb, prefixLen, field, envName.value(), envName.prefix(), getEnv);
            }
          }
          // If the field is not annotated and no prefix is defined,
          // then we do not read the value from the environment!
        }
      }
      theClass = theClass.getSuperclass();
    } while (theClass != null);
    return (SELF) this;
  }

  private void setFromEnv(
      @NotNull StringBuilder sb,
      int prefixLen,
      @NotNull Field field,
      @NotNull String name,
      boolean withPrefix,
      @NotNull Function<@NotNull String, @Nullable String> getEnv
  ) {
    sb.setLength(prefixLen);
    for (int i = 0; i < name.length(); i++) {
      sb.append(Character.toUpperCase(name.charAt(i)));
    }
    final String envName = withPrefix ? sb.toString() : sb.substring(prefixLen, sb.length());
    final String envValue = getEnv.apply(envName);
    if (envValue != null) {
      setField(field, envValue);
    }
  }

  private void setField(@NotNull Field field, @Nullable Object value) {
    if (value instanceof String) {
      value = nullable((String) value);
    }
    try {
      final Class<?> fieldType = field.getType();
      if (fieldType == int.class) {
        if (value instanceof String) {
          final int intValue = Integer.parseInt((String) value, 10);
          field.setInt(this, intValue);
        } else if (value instanceof Number) {
          field.setInt(this, ((Number) value).intValue());
        }
      } else if (fieldType == String.class) {
        if (value == null) {
          field.set(this, null);
        } else if (value instanceof String) {
          field.set(this, value);
        } else if (value instanceof Number) {
          field.set(this, value.toString());
        }
      } else if (fieldType == boolean.class) {
        if (value instanceof String) {
          final String stringValue = (String) value;
          final boolean boolValue =
              !"false".equalsIgnoreCase(stringValue) && !"null".equalsIgnoreCase(stringValue);
          field.setBoolean(this, boolValue);
        } else if (value instanceof Number) {
          field.setBoolean(this, ((Number) value).intValue() != 0);
        } else if (value instanceof Boolean) {
          field.setBoolean(this, (Boolean) value);
        }
      }
    } catch (NullPointerException | NumberFormatException | IllegalAccessException ignore) {
    }
  }
}
