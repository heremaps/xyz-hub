package com.here.xyz.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.function.Function;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Base class of a configuration file that is read from a JSON file with optional override via environment variables. The environment
 * variables are all required to be upper-cased and are optionally prefixed by a defined string. The configuration file should contain the
 * member names in exact notation, unless annotated.
 *
 * @param <SELF> this type.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class JsonConfigFile<SELF extends JsonConfigFile<SELF>> {

  /**
   * Returns the name of the environment variable that should be used to define the search path for the configuration files.
   * <p>
   * Optionally it is possible to add an {@link EnvName} annotation to the class, then this defines the name of the environment variable
   * that holds the search path.
   * <p>
   * In a nutshell, by default the method returns {@code XYZ_CONFIG_PATH}, if the {@link EnvName} annotation is found at the given class
   * type, for example {@code @EnvName("MY_CONFIG_PATH")}, then the method returns exactly this: {@code MY_CONFIG_PATH}.
   *
   * @param classType the class for which to check the annotations.
   * @return the name of the environment variable that holds the config file search path.
   */
  @Nonnull
  public static String configPathEnvName(@Nullable Class<?> classType) {
    while (classType != null) {
      if (classType.isAnnotationPresent(EnvName.class)) {
        final EnvName[] envNames = classType.getAnnotationsByType(EnvName.class);
        return envNames[0].value();
      }
      classType = classType.getSuperclass();
    }
    return "XYZ_CONFIG_PATH";
  }

  /**
   * An optional prefix to be placed in-front of all field names to be read from environment variables. So, the prefix "FOO_" will cause the
   * property "bar" to be looked-up as "FOO_BAR". If no prefix is returned, the environment variable name must explicitly be declared using
   * the annotation {@link EnvName}. Multiple names can be defined and combined with the prefix.
   *
   * @return a prefix or null.
   */
  @Nullable
  protected String envPrefix() {
    return null;
  }

  /**
   * Return the default file name.
   *
   * @return the default file name; null if no such default exists.
   */
  @Nullable
  abstract protected String defaultFile();

  /**
   * A helper method to detect pseudo null values in environment variables. Without this, you can never undefine (unset) a pre-defined value
   * via environment variable. To allow this, this method treats an empty string and the string "null" as null (undefine).
   *
   * @param string the string that is read from the environment variable.
   * @return null if the value represents null; otherwise the given value.
   */
  @Nullable
  public static String nullable(@Nullable String string) {
    return string == null || string.isEmpty() || "null".equalsIgnoreCase(string) ? null : string;
  }

  /**
   * Used for Jackson to parse a JSON object into a map.
   */
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() {
  };

  /**
   * Load the configuration from a disk file, a file in the resources (JAR) or from environment variables. Basically the order is to first
   * read from a disk file, if not found, read from resources. In both cases, eventually the environment variables will override all values
   * being read from the configuration file.
   *
   * @return this.
   * @throws IOException if any error occurred while reading from disk or resources.
   */
  public final SELF load() throws IOException {
    return load(System::getenv);
  }

  /**
   * Load the configuration from a disk file, a file in the resources (JAR) or from environment variables. Basically the order is to first
   * read from a disk file, if not found, read from resources. In both cases, eventually the environment variables will override all values
   * being read from the configuration file.
   *
   * @param getEnv the reference to the method that reads environment variables.
   * @return this.
   * @throws IOException          if any error occurred while reading from disk or resources.
   * @throws NullPointerException if getEnv is null.
   */
  public final SELF load(final @Nonnull Function<String, String> getEnv) throws IOException {
    Map<String, Object> configValues = null;

    final String configPath = nullable(getEnv.apply(configPathEnvName(getClass())));
    final String configFile = nullable(defaultFile());
    if (configFile != null) {
      String filePath;
      try {
        final Path path = (configPath != null ? Paths.get(configPath, configFile) : Paths.get(configFile)).toAbsolutePath();
        final File file = path.toFile();
        if (file.exists() && file.isFile()) {
          filePath = path.toString();
        } else {
          // Try to load from resources (JAR).
          URL url = getClass().getResource(configFile);
          if (url == null && !configFile.startsWith("/")) {
            url = getClass().getResource("/" + configFile);
          }
          filePath = url != null ? url.getPath() : null;
        }
      } catch (final Throwable t) {
        error("Failed to find config file: " + configFile, t);
        filePath = null;
      }

      if (filePath != null) {
        try {
          final InputStream in;
          final int jarEnd = filePath.indexOf(".jar!");
          if (filePath.startsWith("file:") && jarEnd > 0) {
            final String jarPath = filePath.substring("file:".length(), jarEnd + ".jar".length());
            filePath = filePath.substring(jarEnd + ".jar!".length());
            if (filePath.startsWith("/")) {
              filePath = filePath.substring(1);
            }
            info("Load configuration file from JAR, jar-path: " + jarPath + ", file-path: " + filePath);
            //noinspection resource
            final JarFile jarFile = new JarFile(new File(jarPath), false, ZipFile.OPEN_READ);
            final ZipEntry entry = jarFile.getEntry(filePath);
            if (entry != null && entry.isDirectory()) {
              throw new IOException("Config file is directory");
            }
            if (entry == null) {
              throw new FileNotFoundException(filePath);
            }
            in = jarFile.getInputStream(entry);
          } else {
            info("Load configuration file from disk: " + filePath);
            in = Files.newInputStream(new File(filePath).toPath());
          }
          try {
            configValues = new ObjectMapper().readValue(in, MAP_TYPE);
          } finally {
            in.close();
          }
        } catch (Throwable t) {
          error("Failed to load configuration file: " + configFile, t);
        }
      }
    }
    return load(configValues, getEnv);
  }

  /**
   * Read the configuration from the given map and eventually use environment variables to override values.
   *
   * @param defaultValues the map to load values from.
   * @return this.
   */
  @Nonnull
  public final SELF load(@Nonnull Map<String, Object> defaultValues) {
    return load(defaultValues, System::getenv);
  }

  /**
   * Read the configuration from a given map and eventually use environment variables to override all values being read from the map.
   *
   * @param configValues the configuration values to use.
   * @param getEnv       the reference to the method that reads the environment variable; if null, then no environment variables are read.
   * @return this.
   */
  @Nonnull
  public final SELF load(
      @Nullable Map<String, Object> configValues, @Nullable Function<String, String> getEnv) {
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
    //noinspection unchecked
    return (SELF) this;
  }

  private void setFromEnv(
      @Nonnull StringBuilder sb,
      int prefixLen,
      @Nonnull Field field,
      @Nonnull String name,
      boolean withPrefix,
      @Nonnull Function<String, String> getEnv) {
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

  private void setField(@Nonnull Field field, @Nullable Object value) {
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

  /**
   * A method that can be overridden to send information to a logger.
   *
   * @param message the message to log as information.
   */
  protected void info(String message) {
  }

  /**
   * A method that can be overridden to send errors to a logger.
   *
   * @param message the message to log.
   * @param cause   the cause.
   */
  protected void error(String message, Throwable cause) {
  }
}
