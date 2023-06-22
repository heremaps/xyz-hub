package com.here.naksha.lib.core.util;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A file that is read either from disk or resources.
 *
 * @param <SELF> this type.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("unchecked")
public abstract class FileOrResource<SELF extends FileOrResource<SELF>> {

  /** The SLF4j logger. */
  protected static Logger logger = LoggerFactory.getLogger(FileOrResource.class);

  protected FileOrResource(@NotNull String filename) {
    this(filename, null);
  }

  protected FileOrResource(@NotNull String filename, @Nullable String searchPath) {
    this.filename = filename;
    this.searchPath = searchPath;
  }

  @JsonIgnore
  protected @NotNull String filename;

  @JsonIgnore
  protected @Nullable String searchPath;

  public @Nullable String searchPath() {
    return searchPath;
  }

  public SELF withSearchPath(@Nullable String searchPath) {
    this.searchPath = searchPath;
    return (SELF) this;
  }

  public @NotNull String filename() {
    return filename;
  }

  public @NotNull SELF withFilename(@NotNull String filename) {
    final char firstChar = filename.charAt(0);
    if (firstChar != '/' && firstChar != '\\') {
      this.filename = '/' + filename;
    } else {
      this.filename = filename;
    }
    return (SELF) this;
  }

  public @NotNull InputStream open() throws NullPointerException, IOException {
    return open(searchPath());
  }

  public @NotNull InputStream open(@Nullable String searchPath) throws NullPointerException, IOException {
    String filePath = null;
    try {
      final Path path =
          (searchPath != null ? Paths.get(searchPath, filename) : Paths.get(filename)).toAbsolutePath();
      final File file = path.toFile();
      if (file.exists() && file.isFile()) {
        filePath = path.toString();
      } else {
        // Try to load from resources (JAR).
        URL url = getClass().getResource(filename);
        if (url == null) {
          final char firstChar = filename.charAt(0);
          if (firstChar != '/' && firstChar != '\\') {
            url = getClass().getResource('/' + filename);
          }
        }
        filePath = url != null ? url.getPath() : null;
      }
    } catch (final Throwable t) {
      logger.error("Exception while opening file: {}", filename, t);
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
          logger.info("Load file from JAR, jar-path: {}, file-path: {}", jarPath, filePath);
          //noinspection resource
          final JarFile jarFile = new JarFile(new File(jarPath), false, ZipFile.OPEN_READ);
          final ZipEntry entry = jarFile.getEntry(filePath);
          if (entry != null && entry.isDirectory()) {
            throw new IOException("The file is a directory");
          }
          if (entry == null) {
            throw new FileNotFoundException(filePath);
          }
          in = jarFile.getInputStream(entry);
        } else {
          logger.info("Load file from disk: {}", filePath);
          in = Files.newInputStream(new File(filePath).toPath());
        }
        return in;
      } catch (IOException e) {
        throw e;
      } catch (Throwable t) {
        logger.error("Exception while opening file: {}", filename, t);
      }
    }
    throw new FileNotFoundException("Failed to find file: " + filename);
  }

  public static byte @NotNull [] readAllBytes(final @NotNull InputStream in) throws IOException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final byte[] buffer = new byte[1024];
    int read;
    while ((read = in.read(buffer, 0, buffer.length)) >= 0) {
      out.write(buffer, 0, read);
    }
    out.flush();
    return out.toByteArray();
  }

  public byte @NotNull [] readAllBytes() throws IOException {
    try (final InputStream in = open()) {
      return readAllBytes(in);
    }
  }
}
